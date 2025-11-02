#!/usr/bin/env python3
"""
Spatial utilities for PhenoCam site analysis.
"""

import math
from typing import Dict, List, Optional

from .config import EUROPE_BOUNDS, SPATIAL


def is_in_europe(lat: float, lon: float) -> bool:
    """Check if coordinates are within European domain"""
    return (
        EUROPE_BOUNDS["lat_min"] <= lat <= EUROPE_BOUNDS["lat_max"]
        and EUROPE_BOUNDS["lon_min"] <= lon <= EUROPE_BOUNDS["lon_max"]
    )


def filter_european_sites(sites: List[Dict]) -> List[Dict]:
    """Filter sites to European domain only"""
    return [site for site in sites if is_in_europe(site["lat"], site["lon"])]


def create_buffer_bbox(
    lat: float, lon: float, buffer_km: Optional[float] = None
) -> List[float]:
    """Create bounding box around point with buffer in km

    Returns [west, south, east, north] for STAC queries
    """
    if buffer_km is None:
        buffer_km = SPATIAL["aoi_buffer_km"]

    # Convert km to degrees (rough approximation)
    lat_buffer = buffer_km / 111.0
    lon_buffer = buffer_km / (111.0 * math.cos(math.radians(lat)))

    return [
        lon - lon_buffer,  # west
        lat - lat_buffer,  # south
        lon + lon_buffer,  # east
        lat + lat_buffer,  # north
    ]


def validate_coordinates(lat: float, lon: float) -> bool:
    """Validate coordinate ranges"""
    return -90 <= lat <= 90 and -180 <= lon <= 180
