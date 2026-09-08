"""
Tests for date shard calculation logic used in combine_property_data macro.

The macro calculates date shards as YYYYMMDD integers:
- Incremental: earliest_shard = today - static_incremental_days
- Full refresh: earliest_shard = start_date variable
- latest_shard = today (always)

These tests verify the date arithmetic and format conversion logic
without requiring dbt or BigQuery.
"""

import pytest
from datetime import date, timedelta


def date_to_shard(d: date) -> int:
    """
    Convert a date to a YYYYMMDD integer shard.

    This replicates the Jinja logic:
        modules.datetime.date.today()|string|replace("-", "")|int

    Args:
        d: A date object

    Returns:
        Integer in YYYYMMDD format (e.g., 20240115)
    """
    return int(d.isoformat().replace("-", ""))


def calculate_earliest_shard_incremental(today: date, static_incremental_days: int) -> int:
    """
    Calculate earliest shard for incremental mode.

    This replicates the Jinja logic:
        (modules.datetime.date.today() - modules.datetime.timedelta(days=var('static_incremental_days')))|string|replace("-", "")|int

    Args:
        today: The current date
        static_incremental_days: Number of days to look back

    Returns:
        Integer in YYYYMMDD format
    """
    earliest_date = today - timedelta(days=static_incremental_days)
    return date_to_shard(earliest_date)


def calculate_earliest_shard_full_refresh(start_date: str) -> int:
    """
    Calculate earliest shard for full refresh mode.

    This replicates the Jinja logic:
        var('start_date')|int

    Args:
        start_date: Start date as YYYYMMDD string (from dbt variable)

    Returns:
        Integer in YYYYMMDD format
    """
    return int(start_date)


class TestDateToShard:
    """Test the date to YYYYMMDD integer conversion."""

    @pytest.mark.parametrize(
        "input_date,expected_shard",
        [
            (date(2024, 1, 1), 20240101),
            (date(2024, 1, 15), 20240115),
            (date(2024, 12, 31), 20241231),
            (date(2023, 6, 30), 20230630),
            # Single digit months/days get zero-padded
            (date(2024, 1, 9), 20240109),
            (date(2024, 9, 1), 20240901),
        ],
    )
    def test_date_to_shard_format(self, input_date, expected_shard):
        """Verify date converts to correct YYYYMMDD integer."""
        assert date_to_shard(input_date) == expected_shard

    def test_shard_is_integer(self):
        """Verify output is an integer, not a string."""
        result = date_to_shard(date(2024, 1, 15))
        assert isinstance(result, int)


class TestIncrementalShardCalculation:
    """Test earliest_shard calculation for incremental mode."""

    def test_standard_lookback(self):
        """Test typical 3-day lookback (default static_incremental_days)."""
        today = date(2024, 1, 15)
        result = calculate_earliest_shard_incremental(today, static_incremental_days=3)
        # 2024-01-15 minus 3 days = 2024-01-12
        assert result == 20240112

    def test_zero_lookback(self):
        """Test zero-day lookback (process only today)."""
        today = date(2024, 1, 15)
        result = calculate_earliest_shard_incremental(today, static_incremental_days=0)
        assert result == 20240115

    def test_large_lookback(self):
        """Test large lookback period."""
        today = date(2024, 1, 15)
        result = calculate_earliest_shard_incremental(today, static_incremental_days=30)
        # 2024-01-15 minus 30 days = 2023-12-16
        assert result == 20231216

    @pytest.mark.parametrize(
        "today,days_back,expected",
        [
            # Cross month boundary
            (date(2024, 2, 1), 3, 20240129),
            # Cross year boundary
            (date(2024, 1, 1), 3, 20231229),
            # Cross leap year February
            (date(2024, 3, 1), 1, 20240229),  # 2024 is a leap year
            (date(2023, 3, 1), 1, 20230228),  # 2023 is not a leap year
            # End of month
            (date(2024, 1, 31), 31, 20231231),
        ],
    )
    def test_boundary_conditions(self, today, days_back, expected):
        """Test date arithmetic across month/year boundaries."""
        result = calculate_earliest_shard_incremental(today, days_back)
        assert result == expected


class TestFullRefreshShardCalculation:
    """Test earliest_shard calculation for full refresh mode."""

    @pytest.mark.parametrize(
        "start_date_str,expected",
        [
            ("20240101", 20240101),
            ("20230615", 20230615),
            ("20201231", 20201231),
        ],
    )
    def test_start_date_conversion(self, start_date_str, expected):
        """Verify start_date string converts to integer correctly."""
        result = calculate_earliest_shard_full_refresh(start_date_str)
        assert result == expected

    def test_result_is_integer(self):
        """Verify output is an integer, not a string."""
        result = calculate_earliest_shard_full_refresh("20240101")
        assert isinstance(result, int)


class TestShardRangeLogic:
    """Test the overall shard range calculation logic."""

    def test_incremental_range_is_valid(self):
        """Verify earliest_shard <= latest_shard in incremental mode."""
        today = date(2024, 1, 15)
        static_incremental_days = 3

        earliest = calculate_earliest_shard_incremental(today, static_incremental_days)
        latest = date_to_shard(today)

        assert earliest <= latest
        assert earliest == 20240112
        assert latest == 20240115

    def test_full_refresh_range_is_valid(self):
        """Verify earliest_shard <= latest_shard in full refresh mode."""
        today = date(2024, 1, 15)
        start_date = "20230101"

        earliest = calculate_earliest_shard_full_refresh(start_date)
        latest = date_to_shard(today)

        assert earliest <= latest
        assert earliest == 20230101
        assert latest == 20240115

    def test_shard_comparison_works(self):
        """
        Verify YYYYMMDD integers compare correctly chronologically.

        This is important because the macro uses integer comparison:
            if date_shard|int >= earliest_shard|int and date_shard|int <= latest_shard|int
        """
        jan_1 = 20240101
        jan_15 = 20240115
        feb_1 = 20240201
        dec_31_prev = 20231231

        # Earlier dates are smaller integers
        assert dec_31_prev < jan_1 < jan_15 < feb_1

        # Range checks work as expected
        assert jan_1 <= jan_15 <= feb_1  # jan_15 is in range [jan_1, feb_1]
        assert not (dec_31_prev >= jan_1 and dec_31_prev <= jan_15)  # dec_31 not in range


class TestTableFilteringLogic:
    """
    Test the logic used to filter tables by date shard.

    This replicates the Jinja condition:
        if date_shard|int >= earliest_shard|int and date_shard|int <= latest_shard|int
    """

    def test_table_in_range(self):
        """Tables within the date range should be included."""
        earliest_shard = 20240110
        latest_shard = 20240115

        # These should be included
        assert 20240110 >= earliest_shard and 20240110 <= latest_shard
        assert 20240112 >= earliest_shard and 20240112 <= latest_shard
        assert 20240115 >= earliest_shard and 20240115 <= latest_shard

    def test_table_outside_range(self):
        """Tables outside the date range should be excluded."""
        earliest_shard = 20240110
        latest_shard = 20240115

        # These should be excluded
        assert not (20240109 >= earliest_shard and 20240109 <= latest_shard)
        assert not (20240116 >= earliest_shard and 20240116 <= latest_shard)
        assert not (20231231 >= earliest_shard and 20231231 <= latest_shard)

    def test_extract_shard_from_table_name(self):
        """
        Test extracting date shard from table names.

        This replicates:
            relation.identifier|replace('events_intraday_', '')
            relation.identifier|replace('events_', '')
        """
        # Daily table
        daily_table = "events_20240115"
        daily_shard = int(daily_table.replace("events_", ""))
        assert daily_shard == 20240115

        # Intraday table
        intraday_table = "events_intraday_20240115"
        intraday_shard = int(intraday_table.replace("events_intraday_", ""))
        assert intraday_shard == 20240115
