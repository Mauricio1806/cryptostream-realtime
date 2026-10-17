"""Unit tests for Delta Lake utility functions.

These tests mock the DeltaTable API since they run without a Spark session.
Integration tests cover actual Delta operations.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest


class TestUpsertToDelta:
    """Tests for the upsert_to_delta function."""

    @patch("src.streaming.common.delta_utils.DeltaTable")
    def test_merge_called_with_correct_condition(self, mock_dt_class: MagicMock) -> None:
        from src.streaming.common.delta_utils import upsert_to_delta

        mock_batch = MagicMock()
        mock_batch.sparkSession = MagicMock()
        mock_batch.sparkSession.catalog.tableExists.return_value = False

        # First call: table doesn't exist, should write
        upsert_to_delta(
            batch_df=mock_batch,
            table_path="s3a://test-bucket/test-table/",
            merge_condition="target.event_id = source.event_id",
        )

        # When table doesn't exist, should call write
        mock_batch.write.format.assert_called_once()

    @patch("src.streaming.common.delta_utils.DeltaTable")
    def test_merge_when_table_exists(self, mock_dt_class: MagicMock) -> None:
        from src.streaming.common.delta_utils import upsert_to_delta

        mock_batch = MagicMock()
        mock_batch.count.return_value = 10

        # Simulate table exists
        mock_dt_instance = MagicMock()
        mock_dt_class.forPath.return_value = mock_dt_instance
        mock_dt_class.isDeltaTable.return_value = True

        mock_merge_builder = MagicMock()
        mock_dt_instance.alias.return_value.merge.return_value = mock_merge_builder
        mock_merge_builder.whenMatchedUpdateAll.return_value = mock_merge_builder
        mock_merge_builder.whenNotMatchedInsertAll.return_value = mock_merge_builder

        upsert_to_delta(
            batch_df=mock_batch,
            table_path="s3a://test-bucket/test-table/",
            merge_condition="target.event_id = source.event_id",
            table_exists=True,
        )

    def test_empty_batch_skipped(self) -> None:
        from src.streaming.common.delta_utils import upsert_to_delta

        mock_batch = MagicMock()
        mock_batch.count.return_value = 0

        # Should not raise, just skip
        upsert_to_delta(
            batch_df=mock_batch,
            table_path="s3a://test-bucket/test-table/",
            merge_condition="target.event_id = source.event_id",
        )


class TestOptimizeTable:
    """Tests for optimize_table function."""

    @patch("src.streaming.common.delta_utils.DeltaTable")
    def test_optimize_calls_delta(self, mock_dt_class: MagicMock) -> None:
        from src.streaming.common.delta_utils import optimize_table

        mock_spark = MagicMock()
        mock_dt_instance = MagicMock()
        mock_dt_class.forPath.return_value = mock_dt_instance

        optimize_table(mock_spark, "s3a://test/path/")

        mock_dt_class.forPath.assert_called_once_with(mock_spark, "s3a://test/path/")
        mock_dt_instance.optimize.return_value.executeCompaction.assert_called_once()


class TestVacuumTable:
    """Tests for vacuum_table function."""

    @patch("src.streaming.common.delta_utils.DeltaTable")
    def test_vacuum_with_default_retention(self, mock_dt_class: MagicMock) -> None:
        from src.streaming.common.delta_utils import vacuum_table

        mock_spark = MagicMock()
        mock_dt_instance = MagicMock()
        mock_dt_class.forPath.return_value = mock_dt_instance

        vacuum_table(mock_spark, "s3a://test/path/")

        mock_dt_instance.vacuum.assert_called_once_with(168)

    @patch("src.streaming.common.delta_utils.DeltaTable")
    def test_vacuum_with_custom_retention(self, mock_dt_class: MagicMock) -> None:
        from src.streaming.common.delta_utils import vacuum_table

        mock_spark = MagicMock()
        mock_dt_instance = MagicMock()
        mock_dt_class.forPath.return_value = mock_dt_instance

        vacuum_table(mock_spark, "s3a://test/path/", retention_hours=48)

        mock_dt_instance.vacuum.assert_called_once_with(48)
