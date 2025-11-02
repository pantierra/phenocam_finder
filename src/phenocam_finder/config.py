#!/usr/bin/env python3
"""
Configuration for PhenoCam Satellite Data Finder
"""

from typing import Dict, List, Union

EUROPE_BOUNDS: Dict[str, float] = {
    "lat_min": 35.0,
    "lat_max": 71.0,
    "lon_min": -10.0,
    "lon_max": 40.0,
}

SPATIAL: Dict[str, int] = {
    "aoi_buffer_km": 5,
}

SATELLITE: Dict[str, int] = {
    "max_cloud_cover": 30,
    "default_days_back": 30,
    "default_limit": 10,
}

APIS: Dict[str, str] = {
    "phenocam": "https://phenocam.nau.edu/api/cameras/",
    "stac": "https://stac.dataspace.copernicus.eu/v1/search",
}

COLLECTIONS: Dict[str, str] = {
    "sentinel2": "sentinel-2-l2a",
    "sentinel3": "sentinel-3-olci-2-lfr-ntc",
}

EVALUATION: Dict[str, Union[List[int], int, float, bool]] = {
    "growing_season_months": [4, 5, 6, 7, 8, 9, 10],  # April-October
    "min_scenes_per_month": 2,  # Minimum for Sentinel-2
    "max_gap_days": 16,  # Maximum acceptable gap
    "min_data_completeness": 0.7,  # 70% minimum
    "analysis_period_days": 365,  # Full year analysis
    "expected_s3_daily": 25,  # Expected ~daily S3 coverage
    "long_output": False,  # Include temporal_overlap_dates in output
    "gap_count_threshold": 4,  # Days threshold for counting gaps
    "weighted_gap_tau": 20,  # Exponential decay parameter for WGS (EFAST temporal window)
}
