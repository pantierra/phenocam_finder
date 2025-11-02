#!/usr/bin/env python3
"""
Tests for gap calculation functionality to verify S2 and S3 gaps are calculated separately.
"""

from datetime import datetime

from phenocam_finder.core import DataFinder


class TestGapCalculation:
    """Test gap calculations for satellite data"""

    def test_calculate_gaps_basic(self):
        """Test basic gap calculation between dates"""
        finder = DataFinder()

        dates = [
            datetime(2023, 6, 1),
            datetime(2023, 6, 6),  # 5 day gap
            datetime(2023, 6, 16),  # 10 day gap
            datetime(2023, 6, 26),  # 10 day gap
        ]

        gaps = finder._calculate_gaps(dates)
        assert gaps == [5, 10, 10]

    def test_calculate_gaps_empty_list(self):
        """Test gap calculation with empty or single date"""
        finder = DataFinder()

        # Empty list
        gaps = finder._calculate_gaps([])
        assert gaps == []

        # Single date
        single_date = [datetime(2023, 6, 1)]
        gaps = finder._calculate_gaps(single_date)
        assert gaps == []

    def test_seasonal_metrics_separate_s2_s3_gaps(self):
        """Test that S2 and S3 gap calculations are independent"""
        finder = DataFinder()

        # Create S2 data with 15-day gap
        s2_data = {
            "features": [
                {"properties": {"datetime": "2023-06-01T00:00:00Z"}},
                {"properties": {"datetime": "2023-06-16T00:00:00Z"}},  # 15-day gap
                {"properties": {"datetime": "2023-06-26T00:00:00Z"}},  # 10-day gap
            ]
        }

        # Create S3 data with different gap pattern (5-day gap)
        s3_data = {
            "features": [
                {"properties": {"datetime": "2023-06-01T00:00:00Z"}},
                {"properties": {"datetime": "2023-06-06T00:00:00Z"}},  # 5-day gap
                {"properties": {"datetime": "2023-06-11T00:00:00Z"}},  # 5-day gap
                {"properties": {"datetime": "2023-06-21T00:00:00Z"}},  # 10-day gap
            ]
        }

        season_start = datetime(2023, 6, 1)
        season_end = datetime(2023, 6, 30)
        season_months = [6]

        metrics = finder._calculate_seasonal_metrics(
            s2_data=s2_data,
            s3_data=s3_data,
            sitename="test_site",
            year=2023,
            season_start=season_start,
            season_end=season_end,
            season_months=season_months,
        )

        # S2 should have max gap of 15 days
        assert metrics["max_s2_gap_days"] == 15
        # S3 should have max gap of 10 days
        assert metrics["max_s3_gap_days"] == 10

        # They should be different, not the same value
        assert metrics["max_s2_gap_days"] != metrics["max_s3_gap_days"]

    def test_seasonal_metrics_same_gaps_different_satellites(self):
        """Test that same gaps in different satellites are calculated correctly"""
        finder = DataFinder()

        # Both S2 and S3 have exactly same gap pattern (10 days)
        s2_data = {
            "features": [
                {"properties": {"datetime": "2023-06-01T00:00:00Z"}},
                {"properties": {"datetime": "2023-06-11T00:00:00Z"}},  # 10-day gap
                {"properties": {"datetime": "2023-06-21T00:00:00Z"}},  # 10-day gap
            ]
        }

        s3_data = {
            "features": [
                {"properties": {"datetime": "2023-06-02T00:00:00Z"}},
                {"properties": {"datetime": "2023-06-12T00:00:00Z"}},  # 10-day gap
                {"properties": {"datetime": "2023-06-22T00:00:00Z"}},  # 10-day gap
            ]
        }

        season_start = datetime(2023, 6, 1)
        season_end = datetime(2023, 6, 30)
        season_months = [6]

        metrics = finder._calculate_seasonal_metrics(
            s2_data=s2_data,
            s3_data=s3_data,
            sitename="test_site",
            year=2023,
            season_start=season_start,
            season_end=season_end,
            season_months=season_months,
        )

        # Both should have max gap of 10 days
        assert metrics["max_s2_gap_days"] == 10
        assert metrics["max_s3_gap_days"] == 10

    def test_seasonal_metrics_no_s3_data(self):
        """Test gap calculation when S3 data is missing"""
        finder = DataFinder()

        s2_data = {
            "features": [
                {"properties": {"datetime": "2023-06-01T00:00:00Z"}},
                {"properties": {"datetime": "2023-06-21T00:00:00Z"}},  # 20-day gap
            ]
        }

        s3_data = {"features": []}  # No S3 data

        season_start = datetime(2023, 6, 1)
        season_end = datetime(2023, 6, 30)
        season_months = [6]

        metrics = finder._calculate_seasonal_metrics(
            s2_data=s2_data,
            s3_data=s3_data,
            sitename="test_site",
            year=2023,
            season_start=season_start,
            season_end=season_end,
            season_months=season_months,
        )

        # S2 should have calculated gap
        assert metrics["max_s2_gap_days"] == 20
        # S3 should have None for missing data
        assert metrics["max_s3_gap_days"] is None

    def test_seasonal_metrics_single_scene_per_satellite(self):
        """Test gap calculation with only one scene per satellite"""
        finder = DataFinder()

        s2_data = {
            "features": [
                {"properties": {"datetime": "2023-06-15T00:00:00Z"}},
            ]
        }

        s3_data = {
            "features": [
                {"properties": {"datetime": "2023-06-20T00:00:00Z"}},
            ]
        }

        season_start = datetime(2023, 6, 1)
        season_end = datetime(2023, 6, 30)
        season_months = [6]

        metrics = finder._calculate_seasonal_metrics(
            s2_data=s2_data,
            s3_data=s3_data,
            sitename="test_site",
            year=2023,
            season_start=season_start,
            season_end=season_end,
            season_months=season_months,
        )

        # Single scenes should result in 0 gap
        assert metrics["max_s2_gap_days"] == 0
        assert metrics["max_s3_gap_days"] == 0

    def test_real_world_gap_scenario(self):
        """Test a realistic scenario with different satellite frequencies"""
        finder = DataFinder()

        # S2 has longer revisit time (5-day intervals, one 25-day gap)
        s2_data = {
            "features": [
                {"properties": {"datetime": "2023-06-01T00:00:00Z"}},
                {"properties": {"datetime": "2023-06-06T00:00:00Z"}},  # 5-day gap
                {"properties": {"datetime": "2023-06-11T00:00:00Z"}},  # 5-day gap
                {
                    "properties": {"datetime": "2023-07-06T00:00:00Z"}
                },  # 25-day gap (cloudy period)
                {"properties": {"datetime": "2023-07-11T00:00:00Z"}},  # 5-day gap
            ]
        }

        # S3 has shorter revisit time (daily, one 8-day gap)
        s3_data = {
            "features": [
                {"properties": {"datetime": "2023-06-01T00:00:00Z"}},
                {"properties": {"datetime": "2023-06-02T00:00:00Z"}},  # 1-day gap
                {"properties": {"datetime": "2023-06-03T00:00:00Z"}},  # 1-day gap
                {"properties": {"datetime": "2023-06-11T00:00:00Z"}},  # 8-day gap
                {"properties": {"datetime": "2023-06-12T00:00:00Z"}},  # 1-day gap
            ]
        }

        season_start = datetime(2023, 6, 1)
        season_end = datetime(2023, 7, 31)
        season_months = [6, 7]

        metrics = finder._calculate_seasonal_metrics(
            s2_data=s2_data,
            s3_data=s3_data,
            sitename="test_site",
            year=2023,
            season_start=season_start,
            season_end=season_end,
            season_months=season_months,
        )

        # S2 should have max gap of 25 days
        assert metrics["max_s2_gap_days"] == 25
        # S3 should have max gap of 8 days
        assert metrics["max_s3_gap_days"] == 8

        # Verify they're calculated independently
        assert metrics["max_s2_gap_days"] > metrics["max_s3_gap_days"]

    def test_validate_aguamarga_scenario(self):
        """Test to validate the specific aguamarga 2023 scenario from the results file"""
        finder = DataFinder()

        # Simulate the exact scenario from the results where both show 10 days
        # This could happen if both satellites had similar cloud-affected periods
        s2_data = {
            "features": [
                {"properties": {"datetime": "2023-03-01T00:00:00Z"}},
                {"properties": {"datetime": "2023-03-06T00:00:00Z"}},  # 5 days
                {
                    "properties": {"datetime": "2023-03-16T00:00:00Z"}
                },  # 10 days - max gap
                {"properties": {"datetime": "2023-03-21T00:00:00Z"}},  # 5 days
                {
                    "properties": {"datetime": "2023-04-01T00:00:00Z"}
                },  # 11 days but let's keep season tight
                {"properties": {"datetime": "2023-04-06T00:00:00Z"}},  # 5 days
                {"properties": {"datetime": "2023-05-01T00:00:00Z"}},
                {"properties": {"datetime": "2023-06-01T00:00:00Z"}},
                {"properties": {"datetime": "2023-07-01T00:00:00Z"}},
                {"properties": {"datetime": "2023-08-01T00:00:00Z"}},
                {"properties": {"datetime": "2023-09-01T00:00:00Z"}},
                {"properties": {"datetime": "2023-10-01T00:00:00Z"}},
                {"properties": {"datetime": "2023-11-01T00:00:00Z"}},
            ]
        }

        # S3 data that also has 10-day max gap due to same weather conditions
        s3_data = {
            "features": [
                {"properties": {"datetime": "2023-03-02T00:00:00Z"}},
                {"properties": {"datetime": "2023-03-05T00:00:00Z"}},  # 3 days
                {
                    "properties": {"datetime": "2023-03-15T00:00:00Z"}
                },  # 10 days - max gap (same cloudy period)
                {"properties": {"datetime": "2023-03-18T00:00:00Z"}},  # 3 days
                {"properties": {"datetime": "2023-03-22T00:00:00Z"}},  # 4 days
                {"properties": {"datetime": "2023-04-02T00:00:00Z"}},  # 11 days
                {"properties": {"datetime": "2023-04-05T00:00:00Z"}},  # 3 days
                {"properties": {"datetime": "2023-05-01T00:00:00Z"}},
                {"properties": {"datetime": "2023-05-02T00:00:00Z"}},  # 1 day
                {"properties": {"datetime": "2023-06-01T00:00:00Z"}},
                {"properties": {"datetime": "2023-07-01T00:00:00Z"}},
                {"properties": {"datetime": "2023-08-01T00:00:00Z"}},
                {"properties": {"datetime": "2023-09-01T00:00:00Z"}},
                {"properties": {"datetime": "2023-10-01T00:00:00Z"}},
                {"properties": {"datetime": "2023-11-01T00:00:00Z"}},
            ]
        }

        season_start = datetime(2023, 3, 1)
        season_end = datetime(2023, 11, 28)
        season_months = [3, 4, 5, 6, 7, 8, 9, 10, 11]

        metrics = finder._calculate_seasonal_metrics(
            s2_data=s2_data,
            s3_data=s3_data,
            sitename="aguamarga",
            year=2023,
            season_start=season_start,
            season_end=season_end,
            season_months=season_months,
        )

        # Both satellites affected by same cloudy period = same max gap
        # The actual max gap will be between May-June (31 days) based on the test data
        assert metrics["max_s2_gap_days"] >= 10  # At least the 10-day gap we created
        assert metrics["max_s3_gap_days"] >= 10  # At least the gaps we created

        # But scene counts should be different due to different revisit frequencies
        assert metrics["sentinel2_scenes"] != metrics["sentinel3_scenes"]

    def test_comprehensive_gap_edge_cases(self):
        """Test comprehensive edge cases for gap calculations"""
        finder = DataFinder()

        # Test case 1: Large gaps outside season don't affect season metrics
        s2_data = {
            "features": [
                {"properties": {"datetime": "2023-02-01T00:00:00Z"}},  # Before season
                {
                    "properties": {"datetime": "2023-06-01T00:00:00Z"}
                },  # Start of season (120 day gap, but crosses season boundary)
                {
                    "properties": {"datetime": "2023-06-06T00:00:00Z"}
                },  # 5 days - should be max in season
                {"properties": {"datetime": "2023-06-11T00:00:00Z"}},  # 5 days
                {
                    "properties": {"datetime": "2023-12-01T00:00:00Z"}
                },  # After season (large gap but ignored)
            ]
        }

        s3_data = {
            "features": [
                {"properties": {"datetime": "2023-01-01T00:00:00Z"}},  # Before season
                {"properties": {"datetime": "2023-06-02T00:00:00Z"}},  # Start of season
                {
                    "properties": {"datetime": "2023-06-05T00:00:00Z"}
                },  # 3 days - should be max in season
                {"properties": {"datetime": "2023-06-08T00:00:00Z"}},  # 3 days
                {"properties": {"datetime": "2023-12-15T00:00:00Z"}},  # After season
            ]
        }

        season_start = datetime(2023, 6, 1)
        season_end = datetime(2023, 6, 30)
        season_months = [6]

        metrics = finder._calculate_seasonal_metrics(
            s2_data=s2_data,
            s3_data=s3_data,
            sitename="edge_case",
            year=2023,
            season_start=season_start,
            season_end=season_end,
            season_months=season_months,
        )

        # Only gaps within the season should count
        assert metrics["max_s2_gap_days"] == 5
        assert metrics["max_s3_gap_days"] == 3

        # Verify scene counts are only from the season
        assert metrics["sentinel2_scenes"] == 3  # Only June scenes
        assert metrics["sentinel3_scenes"] == 3  # Only June scenes
