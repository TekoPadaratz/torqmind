"""Unit tests for CDC offset recovery below broker retention."""

from __future__ import annotations

import unittest
from unittest.mock import Mock

from confluent_kafka import TopicPartition

from torqmind_cdc_consumer.main import (
    plan_log_start_seek,
    recover_assignment_offsets,
    recover_stale_assignment,
)


class TestPlanLogStartSeek(unittest.TestCase):
    def test_zero_committed_below_empty_partition_seeks_log_start(self) -> None:
        self.assertEqual(plan_log_start_seek(0, 291_320_428), 291_320_428)

    def test_committed_inside_log_is_untouched(self) -> None:
        self.assertIsNone(plan_log_start_seek(100, 50))
        self.assertIsNone(plan_log_start_seek(50, 50))

    def test_uncommitted_uses_auto_offset_reset(self) -> None:
        self.assertIsNone(plan_log_start_seek(-1, 10))
        self.assertIsNone(plan_log_start_seek(-1001, 291_320_428))

    def test_does_not_jump_to_an_independent_log_end(self) -> None:
        log_start = 80
        log_end = 500
        self.assertEqual(plan_log_start_seek(0, log_start), log_start)
        self.assertNotEqual(plan_log_start_seek(0, log_start), log_end)


class TestRecoverAssignmentOffsets(unittest.TestCase):
    def test_on_assign_repairs_stale_offset_and_keeps_live_partition(self) -> None:
        consumer = Mock()
        consumer.get_watermark_offsets.side_effect = [
            (291_320_428, 291_320_428),
            (10, 80),
        ]
        stale = Mock(offset=0)
        live = Mock(offset=40)
        consumer.committed.side_effect = [[stale], [live]]

        partitions = [
            TopicPartition("torqmind.stg.planodecontas", 0, 0),
            TopicPartition("torqmind.stg.comprovantes", 0, 40),
        ]
        recovered = recover_assignment_offsets(consumer, partitions, reassign=True)

        self.assertEqual(len(recovered), 1)
        self.assertEqual(recovered[0].topic, "torqmind.stg.planodecontas")
        self.assertEqual(recovered[0].offset, 291_320_428)
        consumer.committed.assert_not_called()
        consumer.assign.assert_called_once()
        assigned = consumer.assign.call_args[0][0]
        self.assertEqual(assigned[0].offset, 291_320_428)
        self.assertEqual(assigned[1].offset, 40)
        consumer.commit.assert_called_once()

    def test_poll_repair_does_not_reassign_other_partitions(self) -> None:
        consumer = Mock()
        consumer.get_watermark_offsets.return_value = (50, 50)
        consumer.committed.return_value = [Mock(offset=0)]
        recovered = recover_assignment_offsets(
            consumer,
            [TopicPartition("torqmind.stg.filiais", 0, 0)],
            reassign=False,
        )
        self.assertEqual(recovered[0].offset, 50)
        consumer.assign.assert_not_called()
        consumer.seek.assert_called_once()
        consumer.commit.assert_called_once()


class TestRecoverStaleAssignment(unittest.TestCase):
    def test_checks_each_assigned_partition_once(self) -> None:
        import torqmind_cdc_consumer.main as main_mod

        main_mod._recovered_assignment_keys.clear()
        consumer = Mock()
        consumer.assignment.return_value = [TopicPartition("torqmind.stg.planodecontas", 0, 0)]
        consumer.get_watermark_offsets.return_value = (291_320_428, 291_320_428)
        first = recover_stale_assignment(consumer)
        second = recover_stale_assignment(consumer)
        self.assertEqual(first[0].offset, 291_320_428)
        self.assertEqual(second, [])
        self.assertEqual(consumer.get_watermark_offsets.call_count, 1)


if __name__ == "__main__":
    unittest.main()
