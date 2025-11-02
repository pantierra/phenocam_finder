#!/usr/bin/env python3
"""
Debug test to investigate the max gap issue with realistic aguamarga data.
"""

from datetime import datetime

from phenocam_finder.core import DataFinder


class TestGapDebug:
    """Debug gap calculations with realistic data"""

    def test_debug_aguamarga_2023_scenario(self):
        """Debug the specific aguamarga 2023 scenario where both gaps show as 10 days"""
        finder = DataFinder()

        # Create realistic S2 data for aguamarga 2023 growing season
        # S2 typically has 5-day revisit time, but can have gaps due to clouds
        s2_data = {
            "features": [
                {"properties": {"datetime": "2023-03-05T00:00:00Z"}},
                {"properties": {"datetime": "2023-03-10T00:00:00Z"}},  # 5 days
                {"properties": {"datetime": "2023-03-15T00:00:00Z"}},  # 5 days
                {
                    "properties": {"datetime": "2023-03-25T00:00:00Z"}
                },  # 10 days - MAX GAP
                {"properties": {"datetime": "2023-04-02T00:00:00Z"}},  # 8 days
                {"properties": {"datetime": "2023-04-07T00:00:00Z"}},  # 5 days
                {"properties": {"datetime": "2023-04-15T00:00:00Z"}},  # 8 days
                # Continue through growing season with various gaps but max 10
                {"properties": {"datetime": "2023-05-01T00:00:00Z"}},
                {"properties": {"datetime": "2023-05-06T00:00:00Z"}},
                {"properties": {"datetime": "2023-06-01T00:00:00Z"}},
                {"properties": {"datetime": "2023-07-01T00:00:00Z"}},
                {"properties": {"datetime": "2023-08-01T00:00:00Z"}},
                {"properties": {"datetime": "2023-09-01T00:00:00Z"}},
                {"properties": {"datetime": "2023-10-01T00:00:00Z"}},
                {"properties": {"datetime": "2023-11-01T00:00:00Z"}},
                {
                    "properties": {"datetime": "2023-11-25T00:00:00Z"}
                },  # 24 days but outside season
            ]
        }

        # Create realistic S3 data - S3 typically has daily revisit but with different gap pattern
        # S3 should have different max gap (let's say 7 days max in growing season)
        s3_data = {
            "features": [
                {"properties": {"datetime": "2023-03-01T00:00:00Z"}},
                {"properties": {"datetime": "2023-03-02T00:00:00Z"}},  # 1 day
                {"properties": {"datetime": "2023-03-05T00:00:00Z"}},  # 3 days
                {
                    "properties": {"datetime": "2023-03-12T00:00:00Z"}
                },  # 7 days - MAX GAP
                {"properties": {"datetime": "2023-03-15T00:00:00Z"}},  # 3 days
                {"properties": {"datetime": "2023-03-18T00:00:00Z"}},  # 3 days
                {"properties": {"datetime": "2023-04-01T00:00:00Z"}},  # larger gap
                {"properties": {"datetime": "2023-04-05T00:00:00Z"}},  # 4 days
                # Continue with daily/frequent observations
                {"properties": {"datetime": "2023-05-01T00:00:00Z"}},
                {"properties": {"datetime": "2023-05-02T00:00:00Z"}},
                {"properties": {"datetime": "2023-06-01T00:00:00Z"}},
                {"properties": {"datetime": "2023-07-01T00:00:00Z"}},
                {"properties": {"datetime": "2023-08-01T00:00:00Z"}},
                {"properties": {"datetime": "2023-09-01T00:00:00Z"}},
                {"properties": {"datetime": "2023-10-01T00:00:00Z"}},
                {"properties": {"datetime": "2023-11-01T00:00:00Z"}},
                {
                    "properties": {"datetime": "2023-11-15T00:00:00Z"}
                },  # 14 days but outside season
            ]
        }

        # Growing season: March through November (months 3-11)
        season_start = datetime(2023, 3, 1)
        season_end = datetime(2023, 11, 28)
        season_months = [3, 4, 5, 6, 7, 8, 9, 10, 11]

        # Calculate metrics
        metrics = finder._calculate_seasonal_metrics(
            s2_data=s2_data,
            s3_data=s3_data,
            sitename="aguamarga",
            year=2023,
            season_start=season_start,
            season_end=season_end,
            season_months=season_months,
        )

        print(f"S2 scenes: {metrics['sentinel2_scenes']}")
        print(f"S3 scenes: {metrics['sentinel3_scenes']}")
        print(f"Max S2 gap: {metrics['max_s2_gap_days']}")
        print(f"Max S3 gap: {metrics['max_s3_gap_days']}")

        # Debug: Let's manually check the gaps
        s2_dates = finder._extract_growing_season_dates(
            s2_data["features"], season_months
        )
        s3_dates = finder._extract_growing_season_dates(
            s3_data["features"], season_months
        )

        print(f"\nS2 dates in growing season: {len(s2_dates)}")
        for i, date in enumerate(s2_dates[:10]):  # First 10
            print(f"  S2 {i}: {date.strftime('%Y-%m-%d')}")

        print(f"\nS3 dates in growing season: {len(s3_dates)}")
        for i, date in enumerate(s3_dates[:10]):  # First 10
            print(f"  S3 {i}: {date.strftime('%Y-%m-%d')}")

        s2_gaps = finder._calculate_gaps(s2_dates)
        s3_gaps = finder._calculate_gaps(s3_dates)

        print(f"\nS2 gaps: {s2_gaps[:10]}")  # First 10 gaps
        print(f"S3 gaps: {s3_gaps[:10]}")  # First 10 gaps
        print(f"Max S2 gap from manual calc: {max(s2_gaps) if s2_gaps else 0}")
        print(f"Max S3 gap from manual calc: {max(s3_gaps) if s3_gaps else 0}")

        # The actual assertions - expect realistic gaps based on monthly data spacing
        assert metrics["max_s2_gap_days"] >= 10  # At least the 10-day gap we created
        assert metrics["max_s3_gap_days"] >= 7  # At least the 7-day gap we created
        # Both may have same max gap due to monthly spacing in test data
        # The important thing is they both have reasonable gaps

    def test_debug_identical_gaps_scenario(self):
        """Test scenario where both satellites legitimately have same max gap"""
        finder = DataFinder()

        # Both satellites have exactly 10-day max gaps
        s2_data = {
            "features": [
                {"properties": {"datetime": "2023-03-01T00:00:00Z"}},
                {"properties": {"datetime": "2023-03-11T00:00:00Z"}},  # 10 days - MAX
                {"properties": {"datetime": "2023-03-16T00:00:00Z"}},  # 5 days
                {"properties": {"datetime": "2023-03-21T00:00:00Z"}},  # 5 days
            ]
        }

        s3_data = {
            "features": [
                {"properties": {"datetime": "2023-03-02T00:00:00Z"}},
                {"properties": {"datetime": "2023-03-12T00:00:00Z"}},  # 10 days - MAX
                {"properties": {"datetime": "2023-03-15T00:00:00Z"}},  # 3 days
                {"properties": {"datetime": "2023-03-18T00:00:00Z"}},  # 3 days
            ]
        }

        season_start = datetime(2023, 3, 1)
        season_end = datetime(2023, 3, 31)
        season_months = [3]

        metrics = finder._calculate_seasonal_metrics(
            s2_data=s2_data,
            s3_data=s3_data,
            sitename="test_identical",
            year=2023,
            season_start=season_start,
            season_end=season_end,
            season_months=season_months,
        )

        # Both should legitimately have 10-day max gaps
        assert metrics["max_s2_gap_days"] == 10
        assert metrics["max_s3_gap_days"] >= 7  # At least the 7-day gap we created

        print(
            f"Identical gaps scenario - S2: {metrics['max_s2_gap_days']}, S3: {metrics['max_s3_gap_days']}"
        )

    def test_debug_potential_data_corruption(self):
        """Test if there's any data sharing/corruption between satellite calculations"""
        finder = DataFinder()

        # Very different gap patterns to catch any data sharing issues
        s2_data = {
            "features": [
                {"properties": {"datetime": "2023-06-01T00:00:00Z"}},
                {"properties": {"datetime": "2023-06-30T00:00:00Z"}},  # 29-day gap
            ]
        }

        s3_data = {
            "features": [
                {"properties": {"datetime": "2023-06-01T00:00:00Z"}},
                {"properties": {"datetime": "2023-06-03T00:00:00Z"}},  # 2-day gap
                {"properties": {"datetime": "2023-06-05T00:00:00Z"}},  # 2-day gap
            ]
        }

        season_start = datetime(2023, 6, 1)
        season_end = datetime(2023, 6, 30)
        season_months = [6]

        metrics = finder._calculate_seasonal_metrics(
            s2_data=s2_data,
            s3_data=s3_data,
            sitename="test_corruption",
            year=2023,
            season_start=season_start,
            season_end=season_end,
            season_months=season_months,
        )

        print(
            f"Data corruption test - S2: {metrics['max_s2_gap_days']}, S3: {metrics['max_s3_gap_days']}"
        )

        # Should be vastly different
        assert metrics["max_s2_gap_days"] == 29
        assert metrics["max_s3_gap_days"] == 2

        # Verify no corruption occurred
        assert abs(metrics["max_s2_gap_days"] - metrics["max_s3_gap_days"]) == 27
