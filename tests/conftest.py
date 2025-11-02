#!/usr/bin/env python3
"""
Pytest configuration and fixtures for phenocam_finder tests.
"""

import tempfile
from datetime import datetime, timedelta
from unittest.mock import Mock

import pytest


@pytest.fixture
def temp_cache_dir():
    """Create temporary cache directory"""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir


@pytest.fixture
def sample_european_locations():
    """Sample European PhenoCam locations based on realistic coordinates"""
    return [
        {
            # Bavaria, Germany - temperate forest
            "sitename": "bavarian-forest",
            "lat": 49.0369,
            "lon": 13.4147,
            "description": "Bavarian Forest National Park",
            "vegetation_type": "Forest",
            "date_first": "2020-04-01",  # Forest growing season
            "date_last": "2023-10-31",
        },
        {
            # Finnish Lapland - grassland/tundra
            "sitename": "lapland-grassland",
            "lat": 68.7393,
            "lon": 27.0123,
            "description": "Arctic grassland site",
            "vegetation_type": "Grassland",
            "date_first": "2021-05-15",  # Short Arctic growing season
            "date_last": "2023-09-15",
        },
    ]


@pytest.fixture
def mock_phenocam_response():
    """Mock PhenoCam API response with realistic European locations"""
    return {
        "count": 2,
        "next": None,
        "results": [
            {
                "Sitename": "bavarian-forest",
                "Lat": 49.0369,
                "Lon": 13.4147,
                "active": True,
                "date_first": "2020-04-01",
                "date_last": "2023-10-31",
                "sitemetadata": {
                    "site_description": "Bavarian Forest National Park",
                    "primary_veg_type": "Forest",
                },
            },
            {
                "Sitename": "lapland-grassland",
                "Lat": 68.7393,
                "Lon": 27.0123,
                "active": True,
                "date_first": "2021-05-15",
                "date_last": "2023-09-15",
                "sitemetadata": {
                    "site_description": "Arctic grassland site",
                    "primary_veg_type": "Grassland",
                },
            },
        ],
    }


@pytest.fixture
def mock_satellite_response():
    """Mock STAC API satellite response with realistic patterns"""
    base_date = datetime(2023, 6, 1)
    features = []

    # Create realistic satellite scenes with irregular patterns
    # Sentinel-2 has ~5-day revisit but weather creates gaps
    gaps = [
        0,
        5,
        6,
        12,
        15,
        18,
        25,
        30,
        35,
        42,
    ]  # Irregular gaps due to weather
    cloud_covers = [
        5,
        15,
        85,
        25,
        10,
        95,
        20,
        30,
        8,
        45,
    ]  # Realistic cloud variation

    for i, (gap_days, cloud_pct) in enumerate(zip(gaps, cloud_covers)):
        scene_date = base_date + timedelta(days=gap_days)
        features.append(
            {
                "type": "Feature",
                "id": f"S2A_MSIL2A_20{scene_date.strftime('%y%m%d')}T{i:03d}",
                "properties": {
                    "datetime": scene_date.isoformat() + "Z",
                    "eo:cloud_cover": cloud_pct,
                    "platform": "sentinel-2a",
                },
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [
                        [
                            [10.0, 50.0],
                            [11.0, 50.0],
                            [11.0, 51.0],
                            [10.0, 51.0],
                            [10.0, 50.0],
                        ]
                    ],
                },
            }
        )

    return {
        "type": "FeatureCollection",
        "features": features,
        "numberMatched": len(features),
        "numberReturned": len(features),
    }


@pytest.fixture
def mock_requests_session():
    """Mock requests session for API calls"""
    session = Mock()

    # Mock successful GET for PhenoCam
    get_response = Mock()
    get_response.raise_for_status.return_value = None
    get_response.json.return_value = {
        "count": 1,
        "results": [
            {
                "Sitename": "mock-site",
                "Lat": 50.0,
                "Lon": 10.0,
                "active": True,
                "sitemetadata": {"site_description": "Mock site"},
            }
        ],
    }
    session.get.return_value = get_response

    # Mock successful POST for Satellite
    post_response = Mock()
    post_response.raise_for_status.return_value = None
    post_response.json.return_value = {
        "type": "FeatureCollection",
        "features": [
            {
                "properties": {"datetime": "2023-06-01T00:00:00Z"},
                "id": "mock-feature",
            }
        ],
    }
    session.post.return_value = post_response

    return session


@pytest.fixture
def mock_high_cloud_satellite_response():
    """Mock satellite response with high cloud cover (should score poorly)"""
    base_date = datetime(2023, 6, 1)
    features = []

    # All scenes have high cloud cover (>80%)
    for i in range(5):
        scene_date = base_date + timedelta(days=i * 7)
        features.append(
            {
                "type": "Feature",
                "id": f"cloudy-scene-{i:03d}",
                "properties": {
                    "datetime": scene_date.isoformat() + "Z",
                    "eo:cloud_cover": 85.0 + (i * 2),  # 85-93% cloud cover
                    "platform": "sentinel-2a",
                },
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [
                        [
                            [10.0, 50.0],
                            [11.0, 50.0],
                            [11.0, 51.0],
                            [10.0, 51.0],
                            [10.0, 50.0],
                        ]
                    ],
                },
            }
        )

    return {
        "type": "FeatureCollection",
        "features": features,
        "numberMatched": len(features),
        "numberReturned": len(features),
    }


@pytest.fixture
def mock_sparse_satellite_response():
    """Mock satellite response with very few scenes (should score poorly)"""
    base_date = datetime(2023, 6, 1)
    features = []

    # Only 2 scenes over 2 months (very sparse)
    for i in range(2):
        scene_date = base_date + timedelta(days=i * 60)  # 60 day gaps
        features.append(
            {
                "type": "Feature",
                "id": f"sparse-scene-{i:03d}",
                "properties": {
                    "datetime": scene_date.isoformat() + "Z",
                    "eo:cloud_cover": 20.0,  # Good cloud cover but very sparse
                    "platform": "sentinel-2a",
                },
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [
                        [
                            [10.0, 50.0],
                            [11.0, 50.0],
                            [11.0, 51.0],
                            [10.0, 51.0],
                            [10.0, 50.0],
                        ]
                    ],
                },
            }
        )

    return {
        "type": "FeatureCollection",
        "features": features,
        "numberMatched": len(features),
        "numberReturned": len(features),
    }
