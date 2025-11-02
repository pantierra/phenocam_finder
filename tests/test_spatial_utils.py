#!/usr/bin/env python3
"""
Tests for spatial utilities functionality.
"""

import math

from phenocam_finder.spatial_utils import (
    create_buffer_bbox,
    filter_european_sites,
    is_in_europe,
    validate_coordinates,
)


class TestCreateBufferBbox:
    """Test buffer bounding box creation"""

    def test_default_buffer_5km(self):
        """Test buffer bbox with default 5km buffer"""
        lat, lon = 50.0, 10.0
        bbox = create_buffer_bbox(lat, lon)

        # Expected calculations:
        # lat_buffer = 5 / 111.0 ≈ 0.045045
        # lon_buffer = 5 / (111.0 * cos(50°)) ≈ 0.070187
        expected_lat_buffer = 5.0 / 111.0
        expected_lon_buffer = 5.0 / (111.0 * math.cos(math.radians(50.0)))

        assert len(bbox) == 4
        assert bbox == [
            lon - expected_lon_buffer,  # west
            lat - expected_lat_buffer,  # south
            lon + expected_lon_buffer,  # east
            lat + expected_lat_buffer,  # north
        ]

        # Verify buffer is symmetric
        assert abs(bbox[2] - lon) == abs(lon - bbox[0])  # east-west symmetry
        assert abs(bbox[3] - lat) == abs(lat - bbox[1])  # north-south symmetry

    def test_custom_buffer_10km(self):
        """Test buffer bbox with custom 10km buffer"""
        lat, lon = 45.0, 8.0
        buffer_km = 10.0
        bbox = create_buffer_bbox(lat, lon, buffer_km)

        expected_lat_buffer = buffer_km / 111.0
        expected_lon_buffer = buffer_km / (111.0 * math.cos(math.radians(lat)))

        expected_bbox = [
            lon - expected_lon_buffer,  # west
            lat - expected_lat_buffer,  # south
            lon + expected_lon_buffer,  # east
            lat + expected_lat_buffer,  # north
        ]

        assert bbox == expected_bbox

    def test_buffer_at_equator(self):
        """Test buffer at equator where cos(0) = 1"""
        lat, lon = 0.0, 0.0
        buffer_km = 5.0
        bbox = create_buffer_bbox(lat, lon, buffer_km)

        # At equator, lat and lon buffers should be nearly equal
        lat_buffer = buffer_km / 111.0
        lon_buffer = buffer_km / (111.0 * math.cos(math.radians(0.0)))  # cos(0) = 1

        assert abs(lat_buffer - lon_buffer) < 1e-10  # Should be equal
        assert bbox == [-lat_buffer, -lat_buffer, lat_buffer, lat_buffer]

    def test_buffer_at_high_latitude(self):
        """Test buffer at high latitude where longitude buffer increases"""
        lat, lon = 70.0, 15.0
        buffer_km = 5.0
        bbox = create_buffer_bbox(lat, lon, buffer_km)

        lat_buffer = buffer_km / 111.0
        lon_buffer = buffer_km / (111.0 * math.cos(math.radians(70.0)))

        # At high latitude, longitude buffer should be much larger than latitude buffer
        assert lon_buffer > lat_buffer * 2

        expected_bbox = [
            lon - lon_buffer,
            lat - lat_buffer,
            lon + lon_buffer,
            lat + lat_buffer,
        ]
        assert bbox == expected_bbox

    def test_buffer_bbox_format(self):
        """Test that bbox format matches STAC requirements [west, south, east, north]"""
        lat, lon = 52.0, 5.0
        bbox = create_buffer_bbox(lat, lon)

        west, south, east, north = bbox

        # Verify proper ordering
        assert west < east  # west < east
        assert south < north  # south < north
        assert west < lon < east  # point longitude within bounds
        assert south < lat < north  # point latitude within bounds


class TestIsInEurope:
    """Test European domain filtering"""

    def test_point_in_europe(self):
        """Test points within European bounds"""
        # Central Europe
        assert is_in_europe(50.0, 10.0) is True
        # Northern Europe
        assert is_in_europe(65.0, 25.0) is True
        # Southern Europe
        assert is_in_europe(40.0, 15.0) is True
        # Western Europe
        assert is_in_europe(55.0, -5.0) is True

    def test_point_outside_europe(self):
        """Test points outside European bounds"""
        # North America
        assert is_in_europe(40.0, -100.0) is False
        # Asia
        assert is_in_europe(35.0, 100.0) is False
        # Africa
        assert is_in_europe(0.0, 20.0) is False
        # Too far north
        assert is_in_europe(80.0, 10.0) is False

    def test_european_boundary_edges(self):
        """Test points at European boundary edges"""
        # Use actual EUROPE_BOUNDS config values
        lat_min, lat_max = 35.0, 71.0
        lon_min, lon_max = -10.0, 40.0

        # Points just inside boundaries should be True
        assert is_in_europe(lat_min + 0.1, lon_min + 0.1) is True
        assert is_in_europe(lat_max - 0.1, lon_max - 0.1) is True


class TestFilterEuropeanSites:
    """Test European site filtering"""

    def test_filter_mixed_sites(self):
        """Test filtering mixed European and non-European sites"""
        sites = [
            {"sitename": "european1", "lat": 50.0, "lon": 10.0},
            {"sitename": "american1", "lat": 40.0, "lon": -100.0},
            {"sitename": "european2", "lat": 55.0, "lon": 15.0},
            {"sitename": "asian1", "lat": 35.0, "lon": 100.0},
        ]

        european_sites = filter_european_sites(sites)

        assert len(european_sites) == 2
        sitenames = [site["sitename"] for site in european_sites]
        assert "european1" in sitenames
        assert "european2" in sitenames
        assert "american1" not in sitenames
        assert "asian1" not in sitenames

    def test_filter_all_european_sites(self):
        """Test filtering when all sites are European"""
        sites = [
            {"sitename": "site1", "lat": 50.0, "lon": 10.0},
            {"sitename": "site2", "lat": 55.0, "lon": 15.0},
        ]

        european_sites = filter_european_sites(sites)
        assert len(european_sites) == 2
        assert european_sites == sites

    def test_filter_no_european_sites(self):
        """Test filtering when no sites are European"""
        sites = [
            {"sitename": "american1", "lat": 40.0, "lon": -100.0},
            {"sitename": "asian1", "lat": 35.0, "lon": 100.0},
        ]

        european_sites = filter_european_sites(sites)
        assert len(european_sites) == 0

    def test_filter_empty_list(self):
        """Test filtering empty site list"""
        european_sites = filter_european_sites([])
        assert len(european_sites) == 0


class TestValidateCoordinates:
    """Test coordinate validation"""

    def test_valid_coordinates(self):
        """Test valid coordinate ranges"""
        assert validate_coordinates(0.0, 0.0) is True
        assert validate_coordinates(45.0, 90.0) is True
        assert validate_coordinates(-45.0, -90.0) is True
        assert validate_coordinates(90.0, 180.0) is True
        assert validate_coordinates(-90.0, -180.0) is True

    def test_invalid_latitude(self):
        """Test invalid latitude values"""
        assert validate_coordinates(91.0, 0.0) is False
        assert validate_coordinates(-91.0, 0.0) is False
        assert validate_coordinates(100.0, 50.0) is False

    def test_invalid_longitude(self):
        """Test invalid longitude values"""
        assert validate_coordinates(0.0, 181.0) is False
        assert validate_coordinates(0.0, -181.0) is False
        assert validate_coordinates(45.0, 200.0) is False

    def test_boundary_coordinates(self):
        """Test coordinates at valid boundaries"""
        assert validate_coordinates(90.0, 0.0) is True
        assert validate_coordinates(-90.0, 0.0) is True
        assert validate_coordinates(0.0, 180.0) is True
        assert validate_coordinates(0.0, -180.0) is True


class TestBufferIntegration:
    """Integration tests for buffer functionality"""

    def test_buffer_preserves_point_in_center(self):
        """Test that original point remains in center of buffer"""
        lat, lon = 48.5, 9.2
        buffer_km = 7.5
        bbox = create_buffer_bbox(lat, lon, buffer_km)

        west, south, east, north = bbox

        # Center should be original point
        center_lat = (north + south) / 2
        center_lon = (east + west) / 2

        assert abs(center_lat - lat) < 1e-10
        assert abs(center_lon - lon) < 1e-10

    def test_buffer_size_scales_with_distance(self):
        """Test that larger buffers create larger bounding boxes"""
        lat, lon = 50.0, 10.0

        bbox_5km = create_buffer_bbox(lat, lon, 5.0)
        bbox_10km = create_buffer_bbox(lat, lon, 10.0)

        # 10km buffer should be larger in all directions
        assert bbox_10km[0] < bbox_5km[0]  # west extends further
        assert bbox_10km[1] < bbox_5km[1]  # south extends further
        assert bbox_10km[2] > bbox_5km[2]  # east extends further
        assert bbox_10km[3] > bbox_5km[3]  # north extends further

    def test_buffer_consistency_different_locations(self):
        """Test buffer calculations are consistent across different locations"""
        buffer_km = 5.0

        # Test at different latitudes
        locations = [(30.0, 10.0), (45.0, 10.0), (60.0, 10.0)]

        for lat, lon in locations:
            bbox = create_buffer_bbox(lat, lon, buffer_km)
            west, south, east, north = bbox

            # All should have same latitude buffer (independent of longitude)
            lat_buffer = (north - south) / 2
            expected_lat_buffer = buffer_km / 111.0
            assert abs(lat_buffer - expected_lat_buffer) < 1e-10

            # Longitude buffer should vary by latitude
            lon_buffer = (east - west) / 2
            expected_lon_buffer = buffer_km / (111.0 * math.cos(math.radians(lat)))
            assert abs(lon_buffer - expected_lon_buffer) < 1e-10
