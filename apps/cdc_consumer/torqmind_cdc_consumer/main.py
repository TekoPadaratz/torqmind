"""Main entry point for TorqMind CDC Consumer."""

from __future__ import annotations

import signal
import sys
import threading
import time

from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition

from .clickhouse_writer import ClickHouseWriter
from .config import settings
from .debezium import parse_debezium_event
from .logging import get_logger, setup_logging
from .mart_builder import MartBuilder
from .state import ConsumerState

logger = get_logger("main")

_shutdown = False
_recovered_assignment_keys: set[tuple[str, int]] = set()


class MartRefreshWorker:
    """Runs mart refreshes off the consumer hot path."""

    def __init__(self, mart_builder: MartBuilder) -> None:
        self._mart_builder = mart_builder
        self._pending: set[tuple[int, int, int, str]] = set()
        self._pending_lock = threading.Lock()
        self._wake_event = threading.Event()
        self._stop_event = threading.Event()
        self._thread = threading.Thread(
            target=self._run,
            name="mart-refresh-worker",
            daemon=True,
        )

    def start(self) -> None:
        if self._mart_builder.enabled:
            self._thread.start()

    def mark_affected(self, id_empresa: int, id_filial: int, data_key: int, table: str) -> None:
        if not self._mart_builder.enabled:
            return
        with self._pending_lock:
            self._pending.add((id_empresa, id_filial, data_key, table))

    def request_refresh(self) -> None:
        if self._mart_builder.enabled:
            self._wake_event.set()

    def stop(self) -> None:
        if not self._mart_builder.enabled:
            return
        self._stop_event.set()
        self._wake_event.set()
        self._thread.join()

    def _drain_pending(self) -> list[tuple[int, int, int, str]]:
        with self._pending_lock:
            pending = list(self._pending)
            self._pending.clear()
        return pending

    def _has_pending(self) -> bool:
        with self._pending_lock:
            return bool(self._pending)

    def _run(self) -> None:
        while True:
            self._wake_event.wait(timeout=1.0)
            self._wake_event.clear()

            pending = self._drain_pending()
            if pending:
                for id_empresa, id_filial, data_key, table in pending:
                    self._mart_builder.mark_affected(
                        id_empresa=id_empresa,
                        id_filial=id_filial,
                        data_key=data_key,
                        table=table,
                    )

                try:
                    results = self._mart_builder.refresh_if_needed()
                    if results:
                        refreshed = [r.mart_name for r in results if r.error is None]
                        errors = [r for r in results if r.error is not None]
                        if refreshed:
                            logger.info("marts_refreshed", marts=refreshed)
                        if errors:
                            logger.warning(
                                "mart_refresh_partial_failure",
                                failed=[r.mart_name for r in errors],
                                error=errors[0].error[:200] if errors else "",
                            )
                except Exception as e:
                    logger.warning("mart_refresh_failed", error=str(e)[:200])

            if self._stop_event.is_set() and not self._has_pending():
                break


def _signal_handler(signum: int, frame: object) -> None:
    global _shutdown
    _shutdown = True
    logger.info("shutdown_requested", signal=signum)


def create_consumer() -> Consumer:
    """Create and configure the Kafka consumer."""
    conf = {
        "bootstrap.servers": settings.redpanda_brokers,
        "group.id": settings.cdc_consumer_group,
        "auto.offset.reset": settings.cdc_auto_offset_reset,
        "enable.auto.commit": False,
        "max.poll.interval.ms": 300000,
        "session.timeout.ms": 45000,
        "fetch.min.bytes": 1,
        "fetch.wait.max.ms": 500,
    }
    return Consumer(conf)


def get_topics() -> list[str]:
    """Determine which topics to subscribe to."""
    if settings.cdc_topics:
        return [t.strip() for t in settings.cdc_topics.split(",") if t.strip()]
    return []


def plan_log_start_seek(committed_offset: int, log_start: int) -> int | None:
    """Seek target when a committed offset sits below broker retention.

    Retention can raise LOG-START-OFFSET until it equals the high watermark.
    rpk then reports TOTAL-LAG = high - 0 even though the partition is empty.
    Seeking to log_start does not skip live messages: nothing remains below it.
    Never seek to log_end independently of log_start.
    """
    if committed_offset < 0:
        return None
    if committed_offset >= log_start:
        return None
    return log_start


def recover_assignment_offsets(
    consumer: Consumer,
    partitions: list[TopicPartition],
    *,
    reassign: bool = True,
) -> list[TopicPartition]:
    """Move committed offsets that sit below broker retention up to log-start.

    Does not seek to log-end independently and does not drop live messages.
    When reassign is false (poll-time OFFSET_OUT_OF_RANGE), only seek the
    recovered partitions so the rest of the assignment stays intact.
    """
    assigned: list[TopicPartition] = []
    recovered: list[TopicPartition] = []
    try:
        for partition in partitions:
            watermarks = consumer.get_watermark_offsets(
                TopicPartition(partition.topic, partition.partition),
                timeout=10,
            )
            log_start, _log_end = watermarks
            committed_offset = partition.offset
            if committed_offset < 0:
                committed = consumer.committed(
                    [TopicPartition(partition.topic, partition.partition)],
                    timeout=10,
                )
                committed_offset = committed[0].offset if committed else -1
            seek_to = plan_log_start_seek(committed_offset, log_start)
            if seek_to is not None:
                logger.warning(
                    "offset_below_log_start",
                    topic=partition.topic,
                    partition=partition.partition,
                    committed_offset=committed_offset,
                    log_start=log_start,
                )
                repaired = TopicPartition(partition.topic, partition.partition, seek_to)
                assigned.append(repaired)
                recovered.append(repaired)
            else:
                assigned.append(partition)
        if reassign:
            consumer.assign(assigned)
        else:
            for repaired in recovered:
                consumer.seek(repaired)
        if recovered:
            consumer.commit(offsets=recovered, asynchronous=False)
            logger.info("recovered_stale_offsets", count=len(recovered))
        return recovered
    except Exception:
        logger.exception("offset_recovery_failed")
        if reassign:
            consumer.assign(partitions)
        return []


def recover_stale_assignment(consumer: Consumer) -> list[TopicPartition]:
    """Main-thread repair for committed offsets below broker retention.

    Uses the librdkafka default assignor (no on_assign callback). Checking
    assignment() after poll avoids callback deadlocks and still seeks only
    empty retained partitions to log-start.
    """
    assigned = consumer.assignment() or []
    pending = [
        partition
        for partition in assigned
        if (partition.topic, partition.partition) not in _recovered_assignment_keys
    ]
    if not pending:
        return []
    recovered = recover_assignment_offsets(consumer, pending, reassign=False)
    for partition in pending:
        _recovered_assignment_keys.add((partition.topic, partition.partition))
    if pending:
        logger.info("assignment_offsets_checked", count=len(pending), recovered=len(recovered))
    return recovered


def run() -> None:
    """Main consumer loop."""
    global _shutdown

    setup_logging(settings.log_level)
    logger.info(
        "starting",
        brokers=settings.redpanda_brokers,
        group=settings.cdc_consumer_group,
        batch_size=settings.cdc_batch_size,
        flush_interval=settings.cdc_flush_interval_seconds,
    )

    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    consumer = create_consumer()
    writer = ClickHouseWriter()
    state = ConsumerState()
    mart_builder = MartBuilder(
        clickhouse_host=settings.clickhouse_host,
        clickhouse_port=settings.clickhouse_port,
        clickhouse_user=settings.clickhouse_user,
        clickhouse_password=settings.clickhouse_password,
        enabled=getattr(settings, "enable_mart_builder", True),
        source=getattr(settings, "realtime_marts_source", "stg"),
    )
    mart_worker = MartRefreshWorker(mart_builder)
    mart_worker.start()

    # Subscribe
    topics = get_topics()
    if topics:
        consumer.subscribe(topics)
        logger.info("subscribed_topics", topics=topics)
    else:
        consumer.subscribe([settings.cdc_topic_pattern])
        logger.info("subscribed_pattern", pattern=settings.cdc_topic_pattern)

    try:
        while not _shutdown:
            msg = consumer.poll(timeout=settings.cdc_poll_timeout_seconds)
            recover_stale_assignment(consumer)

            if msg is None:
                # No message; check if we should flush
                if writer.should_flush() and writer.buffer_size > 0:
                    _do_flush(writer, consumer, state, mart_worker)
                continue

            error = msg.error()
            if error:
                if error.code() == KafkaError._PARTITION_EOF:
                    continue
                if error.code() == KafkaError._OFFSET_OUT_OF_RANGE:
                    recover_assignment_offsets(
                        consumer,
                        [TopicPartition(msg.topic(), msg.partition())],
                        reassign=False,
                    )
                    continue
                logger.error("kafka_error", error=str(error))
                if error.code() in (KafkaError._ALL_BROKERS_DOWN, KafkaError._FATAL):
                    raise KafkaException(error)
                continue

            # Parse the event
            try:
                event = parse_debezium_event(
                    topic=msg.topic(),
                    partition=msg.partition(),
                    offset=msg.offset(),
                    key_bytes=msg.key(),
                    value_bytes=msg.value(),
                )
            except Exception as e:
                state.increment_errors()
                writer.record_error(
                    topic=msg.topic() or "",
                    partition=msg.partition() or 0,
                    offset=msg.offset() or 0,
                    table_schema="",
                    table_name="",
                    error_type="PARSE_ERROR",
                    error_message=str(e),
                )
                continue

            if event is None:
                # Tombstone or unrecognized message
                continue

            # Process the event
            try:
                writer.process_event(event)
                state.record_offset(event.topic, event.partition, event.offset)
                state.increment_processed()
                # Track affected windows for the async mart refresh worker.
                _record = event.after or event.before or {}
                mart_worker.mark_affected(
                    id_empresa=event.id_empresa,
                    id_filial=int(_record.get("id_filial", 0) or 0),
                    data_key=event.data_key,
                    table=event.table_name or "",
                )
            except Exception as e:
                state.increment_errors()
                writer.record_error(
                    topic=event.topic,
                    partition=event.partition,
                    offset=event.offset,
                    table_schema=event.table_schema,
                    table_name=event.table_name,
                    error_type="PROCESS_ERROR",
                    error_message=str(e),
                )
                if state.events_errors >= settings.max_errors_before_restart:
                    logger.error("max_errors_reached", errors=state.events_errors)
                    break

            # Flush if batch is ready
            if writer.should_flush():
                _do_flush(writer, consumer, state, mart_worker)

    except KeyboardInterrupt:
        logger.info("keyboard_interrupt")
    finally:
        # Final flush
        if writer.buffer_size > 0:
            try:
                _do_flush(writer, consumer, state, mart_worker)
            except Exception as e:
                logger.error("final_flush_failed", error=str(e))

        mart_worker.stop()

        consumer.close()
        logger.info(
            "stopped",
            events_processed=state.events_processed,
            events_errors=state.events_errors,
        )


def _do_flush(writer: ClickHouseWriter, consumer: Consumer, state: ConsumerState, mart_worker: MartRefreshWorker) -> None:
    """Flush buffers, commit offsets, and signal background mart refresh."""
    try:
        rows = writer.flush()
        consumer.commit(asynchronous=False)
        if rows > 0:
            logger.info(
                "flushed",
                rows=rows,
                processed_total=state.events_processed,
                errors_total=state.events_errors,
            )
            mart_worker.request_refresh()
    except Exception as e:
        logger.error("flush_failed", error=str(e))
        raise


if __name__ == "__main__":
    run()
