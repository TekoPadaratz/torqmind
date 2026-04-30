"""Main entry point for TorqMind CDC Consumer."""

from __future__ import annotations

import signal
import sys
import time

from confluent_kafka import Consumer, KafkaError, KafkaException

from .clickhouse_writer import ClickHouseWriter
from .config import settings
from .debezium import parse_debezium_event
from .logging import get_logger, setup_logging
from .state import ConsumerState

logger = get_logger("main")

_shutdown = False


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

    # Subscribe
    topics = get_topics()
    if topics:
        consumer.subscribe(topics)
        logger.info("subscribed_topics", topics=topics)
    else:
        # Use pattern subscription
        consumer.subscribe([settings.cdc_topic_pattern])
        logger.info("subscribed_pattern", pattern=settings.cdc_topic_pattern)

    try:
        while not _shutdown:
            msg = consumer.poll(timeout=settings.cdc_poll_timeout_seconds)

            if msg is None:
                # No message; check if we should flush
                if writer.should_flush() and writer.buffer_size > 0:
                    _do_flush(writer, consumer, state)
                continue

            error = msg.error()
            if error:
                if error.code() == KafkaError._PARTITION_EOF:
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
                _do_flush(writer, consumer, state)

    except KeyboardInterrupt:
        logger.info("keyboard_interrupt")
    finally:
        # Final flush
        if writer.buffer_size > 0:
            try:
                _do_flush(writer, consumer, state)
            except Exception as e:
                logger.error("final_flush_failed", error=str(e))

        consumer.close()
        logger.info(
            "stopped",
            events_processed=state.events_processed,
            events_errors=state.events_errors,
        )


def _do_flush(writer: ClickHouseWriter, consumer: Consumer, state: ConsumerState) -> None:
    """Flush buffers and commit offsets."""
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
    except Exception as e:
        logger.error("flush_failed", error=str(e))
        raise


if __name__ == "__main__":
    run()
