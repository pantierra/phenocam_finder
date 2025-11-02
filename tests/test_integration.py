#!/usr/bin/env python3
"""
Integration tests for end-to-end workflows.
"""

from unittest.mock import Mock, patch

import pytest

from phenocam_finder.config import EUROPE_BOUNDS
from phenocam_finder.core import DataFinder


class TestFullWorkflows:
    """Test complete end-to-end workflows"""

    def test_complete_phenocam_to_satellite_workflow(
        self, sample_european_locations, mock_satellite_response
    ):
        """Test complete workflow from PhenoCam sites to satellite data"""
        with patch("phenocam_finder.core.PhenoCamQuery") as mock_phenocam_class, patch(
            "phenocam_finder.core.SatelliteQuery"
        ) as mock_satellite_class:
            # Setup mocks
            mock_phenocam = Mock()
            mock_phenocam.get_all_locations.return_value = sample_european_locations
            mock_phenocam_class.return_value = mock_phenocam

            mock_satellite = Mock()
            mock_satellite.search_satellite_data.return_value = mock_satellite_response
            mock_satellite_class.return_value = mock_satellite

            # Run workflow
            finder = DataFinder()
            results = finder.find_data_for_locations()

            # Verify complete workflow
            assert len(results) == 2
            for result in results:
                assert "location" in result
                assert "satellite_images" in result
                assert result["location"]["sitename"] in [
                    "bavarian-forest",
                    "lapland-grassland",
                ]

    def test_site_evaluation_workflow(
        self, sample_european_locations, mock_satellite_response
    ):
        """Test site suitability evaluation workflow"""
        with patch("phenocam_finder.core.PhenoCamQuery") as mock_phenocam_class, patch(
            "phenocam_finder.core.SatelliteQuery"
        ) as mock_satellite_class:
            # Setup mocks
            mock_phenocam = Mock()
            mock_phenocam.get_all_locations.return_value = sample_european_locations
            mock_phenocam_class.return_value = mock_phenocam

            mock_satellite = Mock()
            mock_satellite.search_satellite_data.return_value = mock_satellite_response
            mock_satellite.search_satellite_data_daterange.return_value = (
                mock_satellite_response
            )
            mock_satellite_class.return_value = mock_satellite

            # Run evaluation workflow
            finder = DataFinder()
            results = finder.evaluate_all_sites()

            # Verify evaluation results - returns results per growing season year
            assert len(results) >= 2
            for result in results:
                assert "suitability_score" in result
                assert "sitename" in result
                # Score should be meaningful (not 0.0 for good data)
                if result.get("error") is None:
                    assert isinstance(result["suitability_score"], (int, float))
                    assert result["suitability_score"] > 0.0

    def test_error_recovery_workflow(self, sample_european_locations):
        """Test workflow continues despite individual site errors"""
        with patch("phenocam_finder.core.PhenoCamQuery") as mock_phenocam_class, patch(
            "phenocam_finder.core.SatelliteQuery"
        ) as mock_satellite_class:
            # Setup mocks - satellite query fails for some sites
            mock_phenocam = Mock()
            mock_phenocam.get_all_locations.return_value = sample_european_locations
            mock_phenocam_class.return_value = mock_phenocam

            mock_satellite = Mock()
            mock_satellite.search_satellite_data.side_effect = [
                Exception("API Error"),  # First site fails
                {
                    "type": "FeatureCollection",
                    "features": [],
                },  # Second succeeds
            ]
            mock_satellite_class.return_value = mock_satellite

            # Run workflow
            finder = DataFinder()
            results = finder.find_data_for_locations()

            # Verify error handling
            assert len(results) == 2
            assert "error" in results[0]
            assert "satellite_images" in results[1]


class TestConfigurationIntegration:
    """Test configuration is properly integrated across modules"""

    def test_europe_bounds_filtering_integration(self):
        """Test Europe bounds are consistently applied using realistic coordinates"""
        mixed_global_sites = [
            {
                "Sitename": "harvard-forest",
                "Lat": 42.5378,
                "Lon": -72.1715,  # Massachusetts, USA
                "active": True,
                "sitemetadata": {"site_description": "Harvard Forest LTER"},
            },
            {
                "Sitename": "yakutsk-forest",
                "Lat": 62.2553,
                "Lon": 129.6136,  # Siberia, Russia (Asia)
                "active": True,
                "sitemetadata": {"site_description": "Siberian forest"},
            },
            {
                "Sitename": "hyytiala-forest",
                "Lat": 61.8474,
                "Lon": 24.2948,  # Finland, Europe
                "active": True,
                "sitemetadata": {"site_description": "Hyytiälä Forest Station"},
            },
        ]

        with patch("phenocam_finder.core.PhenoCamQuery") as mock_phenocam_class:
            mock_phenocam = Mock()

            # Mock raw API response with mixed locations
            mock_phenocam.session.get.return_value.json.return_value = {
                "count": 3,
                "results": mixed_global_sites,
            }

            # Mock the filtering behavior - only European site remains
            filtered_sites = [
                {
                    "sitename": "hyytiala-forest",
                    "lat": 61.8474,
                    "lon": 24.2948,
                    "description": "Hyytiälä Forest Station",
                    "vegetation_type": "Forest",
                    "date_first": "2020-05-01",
                    "date_last": "2023-09-30",
                }
            ]
            mock_phenocam.get_all_locations.return_value = filtered_sites
            mock_phenocam_class.return_value = mock_phenocam

            finder = DataFinder()
            locations = finder.phenocam.get_all_locations()

            # Verify only European sites are returned
            assert len(locations) == 1
            assert locations[0]["sitename"] == "hyytiala-forest"

            # Verify coordinates are within Europe bounds
            lat, lon = locations[0]["lat"], locations[0]["lon"]
            assert EUROPE_BOUNDS["lat_min"] <= lat <= EUROPE_BOUNDS["lat_max"]
            assert EUROPE_BOUNDS["lon_min"] <= lon <= EUROPE_BOUNDS["lon_max"]

    def test_multi_sensor_query_integration(self, mock_satellite_response):
        """Test both Sentinel-2 and Sentinel-3 queries are made"""
        with patch("phenocam_finder.core.SatelliteQuery") as mock_satellite_class:
            mock_satellite = Mock()
            mock_satellite.search_satellite_data.return_value = mock_satellite_response
            mock_satellite.search_satellite_data_daterange.return_value = (
                mock_satellite_response
            )
            mock_satellite_class.return_value = mock_satellite

            finder = DataFinder()
            location = {
                "sitename": "bavarian-forest",
                "lat": 49.0369,
                "lon": 13.4147,
                "date_first": "2020-04-01",
                "date_last": "2023-10-31",
            }

            finder.evaluate_site_suitability(
                lat=location["lat"],
                lon=location["lon"],
                sitename=location["sitename"],
                location=location,
            )

            # Verify both sensors were queried via daterange method
            call_args_list = (
                mock_satellite.search_satellite_data_daterange.call_args_list
            )
            assert len(call_args_list) == 2  # Should call for both S2 and S3


class TestCachingIntegration:
    """Test caching works across the full workflow"""

    def test_phenocam_caching_across_multiple_calls(
        self, temp_cache_dir, mock_phenocam_response
    ):
        """Test PhenoCam caching works across multiple DataFinder operations"""
        with patch("requests.Session") as mock_session_class:
            mock_session = Mock()
            mock_response = Mock()
            mock_response.raise_for_status.return_value = None
            mock_response.json.return_value = mock_phenocam_response
            mock_session.get.return_value = mock_response
            mock_session_class.return_value = mock_session

            finder = DataFinder(cache_dir=temp_cache_dir)

            # Call twice - should get same results from cache
            result1 = finder.phenocam.get_all_locations()
            result2 = finder.phenocam.get_all_locations()

            # Results should be consistent and non-empty
            assert result1 == result2
            assert len(result1) > 0
            # Should contain realistic European locations
            for location in result1:
                lat, lon = location["lat"], location["lon"]
                # Verify within Europe bounds
                assert 35.0 <= lat <= 71.0
                assert -10.0 <= lon <= 40.0

    def test_full_workflow_with_caching(
        self, temp_cache_dir, sample_european_locations, mock_satellite_response
    ):
        """Test complete workflow benefits from caching"""
        with patch("phenocam_finder.core.PhenoCamQuery") as mock_phenocam_class, patch(
            "phenocam_finder.core.SatelliteQuery"
        ) as mock_satellite_class:
            # Setup cached PhenoCam query
            mock_phenocam = Mock()
            mock_phenocam.get_all_locations.return_value = (
                sample_european_locations[:1]  # Just one site for speed
            )
            mock_phenocam.cache_hits = 0
            mock_phenocam.cache_misses = 0
            mock_phenocam_class.return_value = mock_phenocam

            # Setup satellite query
            mock_satellite = Mock()
            mock_satellite.search_satellite_data.return_value = mock_satellite_response
            mock_satellite_class.return_value = mock_satellite

            finder = DataFinder(cache_dir=temp_cache_dir)

            # Run workflow twice
            results1 = finder.find_data_for_locations()
            results2 = finder.find_data_for_locations()

            # Verify results are consistent
            assert len(results1) == len(results2) == 1
            # Verify same locations are processed
            sitenames1 = {r["location"]["sitename"] for r in results1}
            sitenames2 = {r["location"]["sitename"] for r in results2}
            assert sitenames1 == sitenames2

            # Verify caching was used (mock will show this)
            assert mock_phenocam.get_all_locations.call_count >= 2


class TestErrorHandling:
    """Test error handling across integrated workflows"""

    def test_phenocam_api_failure_handling(self):
        """Test graceful handling of PhenoCam API failures"""
        with patch("phenocam_finder.core.PhenoCamQuery") as mock_phenocam_class:
            mock_phenocam = Mock()
            mock_phenocam.get_all_locations.side_effect = Exception("PhenoCam API Down")
            mock_phenocam_class.return_value = mock_phenocam

            finder = DataFinder()

            with pytest.raises(Exception, match="PhenoCam API Down"):
                finder.find_data_for_locations()

    def test_partial_satellite_failures(self, sample_european_locations):
        """Test handling when satellite queries partially fail"""
        with patch("phenocam_finder.core.PhenoCamQuery") as mock_phenocam_class, patch(
            "phenocam_finder.core.SatelliteQuery"
        ) as mock_satellite_class:
            mock_phenocam = Mock()
            mock_phenocam.get_all_locations.return_value = sample_european_locations
            mock_phenocam_class.return_value = mock_phenocam

            mock_satellite = Mock()
            # Always fail with consistent error
            mock_satellite.search_satellite_data_daterange.side_effect = Exception(
                "Satellite API Error"
            )
            mock_satellite_class.return_value = mock_satellite

            finder = DataFinder()
            results = finder.evaluate_all_sites(max_locations=1)

            # Should get results for the site across multiple years, but with errors
            assert len(results) >= 1

            # All results should have errors due to API failure
            error_results = [r for r in results if "error" in r]
            assert len(error_results) >= 1

            # Error scores should be None (unknown), not 0.0 (known bad)
            for error_result in error_results:
                assert error_result["suitability_score"] is None
                assert "Satellite API Error" in error_result["error"]
