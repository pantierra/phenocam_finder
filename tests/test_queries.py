#!/usr/bin/env python3
"""
Tests for PhenoCam and Satellite API query functionality.
"""

from pathlib import Path
from unittest.mock import Mock, patch

import pytest
import requests

from phenocam_finder.phenocam_query import PhenoCamQuery
from phenocam_finder.satellite_query import SatelliteQuery


class TestPhenoCamQuery:
    """Test PhenoCam API query functionality"""

    def test_init_and_setup(self, temp_cache_dir):
        """Test PhenoCamQuery initialization"""
        query = PhenoCamQuery(cache_dir=temp_cache_dir)
        assert query.cache_dir == Path(temp_cache_dir)
        assert query.api_url == "https://phenocam.nau.edu/api/cameras/"
        assert query.session is not None

    def test_get_all_locations_success(self, mock_phenocam_response):
        """Test successful PhenoCam location retrieval"""
        with patch("requests.Session") as mock_session_class:
            mock_session = Mock()
            mock_response = Mock()
            mock_response.raise_for_status.return_value = None
            mock_response.json.return_value = mock_phenocam_response
            mock_session.get.return_value = mock_response
            mock_session_class.return_value = mock_session

            query = PhenoCamQuery()
            locations = query.get_all_locations()

            # Should filter to European locations only
            assert len(locations) >= 1
            for location in locations:
                assert "sitename" in location
                assert "lat" in location
                assert "lon" in location

    def test_caching_functionality(self, temp_cache_dir, mock_phenocam_response):
        """Test PhenoCam response caching"""
        with patch("requests.Session") as mock_session_class:
            mock_session = Mock()
            mock_response = Mock()
            mock_response.raise_for_status.return_value = None
            mock_response.json.return_value = mock_phenocam_response
            mock_session.get.return_value = mock_response
            mock_session_class.return_value = mock_session

            query = PhenoCamQuery(cache_dir=temp_cache_dir)

            # First call should hit API
            locations1 = query.get_all_locations()
            first_call_count = mock_session.get.call_count

            # Second call should use cache
            locations2 = query.get_all_locations()
            second_call_count = mock_session.get.call_count

            # Results should be the same
            assert locations1 == locations2
            # Should have cached the result
            assert second_call_count == first_call_count  # No additional API calls

    def test_european_filtering(self, temp_cache_dir):
        """Test that only European locations are returned"""
        mixed_response = {
            "count": 3,
            "results": [
                {
                    "Sitename": "european-site",
                    "Lat": 50.0,
                    "Lon": 10.0,  # Europe
                    "active": True,
                    "sitemetadata": {"site_description": "European site"},
                },
                {
                    "Sitename": "us-site",
                    "Lat": 40.0,
                    "Lon": -100.0,  # USA
                    "active": True,
                    "sitemetadata": {"site_description": "US site"},
                },
                {
                    "Sitename": "inactive-site",
                    "Lat": 45.0,
                    "Lon": 15.0,  # Europe but inactive
                    "active": False,
                    "sitemetadata": {"site_description": "Inactive site"},
                },
            ],
        }

        with patch("requests.Session") as mock_session_class:
            mock_session = Mock()
            mock_response = Mock()
            mock_response.raise_for_status.return_value = None
            mock_response.json.return_value = mixed_response
            mock_session.get.return_value = mock_response
            mock_session_class.return_value = mock_session

            query = PhenoCamQuery(cache_dir=temp_cache_dir)
            locations = query.get_all_locations()

            # Should only return the active European site
            assert len(locations) == 1
            assert locations[0]["sitename"] == "european-site"


class TestSatelliteQuery:
    """Test Satellite STAC API query functionality"""

    def test_init_and_setup(self, temp_cache_dir):
        """Test SatelliteQuery initialization"""
        query = SatelliteQuery(cache_dir=temp_cache_dir)
        assert query.cache_dir == Path(temp_cache_dir)
        assert query.stac_url == "https://stac.dataspace.copernicus.eu/v1/search"
        assert query.session is not None

    def test_search_satellite_data_success(self, mock_satellite_response):
        """Test successful satellite data search"""
        with patch("requests.Session") as mock_session_class:
            mock_session = Mock()
            mock_response = Mock()
            mock_response.raise_for_status.return_value = None
            mock_response.json.return_value = mock_satellite_response
            mock_session.post.return_value = mock_response
            mock_session_class.return_value = mock_session

            query = SatelliteQuery()
            data = query.search_satellite_data(
                lat=50.0,
                lon=10.0,
                collection="sentinel-2-l2a",
                days_back=30,
                limit=100,
            )

            assert data["type"] == "FeatureCollection"
            assert len(data["features"]) == 10

    def test_search_satellite_data_api_error(self):
        """Test satellite API error handling"""
        with patch("requests.Session") as mock_session_class:
            mock_session = Mock()
            mock_response = Mock()
            mock_response.raise_for_status.side_effect = requests.RequestException(
                "API Error"
            )
            mock_session.post.return_value = mock_response
            mock_session_class.return_value = mock_session

            query = SatelliteQuery()

            with pytest.raises(requests.RequestException):
                query.search_satellite_data(
                    lat=50.0,
                    lon=10.0,
                    collection="sentinel-2-l2a",
                )

    def test_search_parameters_validation(self):
        """Test that search parameters are properly used"""
        with patch("requests.Session") as mock_session_class:
            mock_session = Mock()
            mock_response = Mock()
            mock_response.raise_for_status.return_value = None
            mock_response.json.return_value = {
                "type": "FeatureCollection",
                "features": [],
            }
            mock_session.post.return_value = mock_response
            mock_session_class.return_value = mock_session

            query = SatelliteQuery()
            query.search_satellite_data(
                lat=50.0,
                lon=10.0,
                collection="sentinel-2-l2a",
                days_back=30,
                limit=100,
            )

            # Verify the API was called
            assert mock_session.post.called
            call_args = mock_session.post.call_args
            assert call_args[1]["json"]["collections"] == ["sentinel-2-l2a"]
            assert call_args[1]["json"]["limit"] == 100
