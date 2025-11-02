#!/usr/bin/env python3
"""
PhenoCam query module for accessing camera locations.
"""

import hashlib
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import requests

from .config import APIS, EUROPE_BOUNDS


class PhenoCamQuery:
    """Query PhenoCam API for European camera locations with caching"""

    def __init__(self, cache_dir: str = ".phenocam_cache"):
        self.session = requests.Session()
        self.api_url = APIS["phenocam"]
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(exist_ok=True)
        self.cache_hits = 0
        self.cache_misses = 0

    def get_all_locations(self) -> List[Dict]:
        """Get all PhenoCam locations with pagination"""
        # Check cache first
        cache_key = self._create_cache_key()
        cached_result = self._get_cached_result(cache_key)

        if cached_result is not None:
            import logging

            logger = logging.getLogger(__name__)
            self.cache_hits += 1
            logger.debug("🗄️  Cache HIT for PhenoCam locations")
            return cached_result

        # Cache miss - fetch from API
        import logging

        logger = logging.getLogger(__name__)
        self.cache_misses += 1
        logger.debug("🌐 API query for PhenoCam locations")

        all_cameras = []
        url = self.api_url

        while url:
            response = self.session.get(url)
            response.raise_for_status()
            data = response.json()

            all_cameras.extend(data["results"])
            url = data.get("next")

        # Process and filter European locations
        european_locations = [
            {
                "sitename": site["Sitename"],
                "lat": site["Lat"],
                "lon": site["Lon"],
                "description": site["sitemetadata"].get("site_description", ""),
                "vegetation_type": site["sitemetadata"].get("primary_veg_type", ""),
                "date_first": site.get("date_first", ""),
                "date_last": site.get("date_last", ""),
            }
            for site in all_cameras
            if site.get("active", False)
            and EUROPE_BOUNDS["lat_min"] <= site["Lat"] <= EUROPE_BOUNDS["lat_max"]
            and EUROPE_BOUNDS["lon_min"] <= site["Lon"] <= EUROPE_BOUNDS["lon_max"]
        ]

        # Cache the result
        self._cache_result(cache_key, european_locations)

        return european_locations

    def _create_cache_key(self) -> str:
        """Create cache key for PhenoCam locations"""
        # Simple key since we're caching all European locations
        cache_data = {
            "api_url": self.api_url,
            "bounds": EUROPE_BOUNDS,
        }
        cache_string = json.dumps(cache_data, sort_keys=True)
        return hashlib.md5(cache_string.encode()).hexdigest()

    def _get_cached_result(self, cache_key: str) -> Optional[List[Dict]]:
        """Retrieve cached PhenoCam locations if valid"""
        cache_file = self.cache_dir / f"{cache_key}.json"

        if not cache_file.exists():
            return None

        try:
            with open(cache_file, "r") as f:
                cached_data = json.load(f)

            # Check if cache is still valid (24 hours for PhenoCam data)
            cache_timestamp = datetime.fromisoformat(cached_data["timestamp"])
            if (datetime.now() - cache_timestamp).total_seconds() > 24 * 3600:
                return None

            return cached_data["result"]

        except (json.JSONDecodeError, KeyError, ValueError):
            # Remove corrupted cache file
            cache_file.unlink(missing_ok=True)
            return None

    def _cache_result(self, cache_key: str, result: List[Dict]) -> None:
        """Cache PhenoCam locations"""
        cache_file = self.cache_dir / f"{cache_key}.json"

        cache_data = {
            "timestamp": datetime.now().isoformat(),
            "result": result,
        }

        try:
            with open(cache_file, "w") as f:
                json.dump(cache_data, f, indent=2)
        except Exception as e:
            # Log but don't fail if caching fails
            import logging

            logger = logging.getLogger(__name__)
            logger.warning(f"Failed to cache PhenoCam locations: {e}")

    def get_cache_stats(self) -> Dict:
        """Get cache performance statistics"""
        total_requests = self.cache_hits + self.cache_misses
        hit_rate = (self.cache_hits / total_requests * 100) if total_requests > 0 else 0

        return {
            "cache_hits": self.cache_hits,
            "cache_misses": self.cache_misses,
            "total_requests": total_requests,
            "hit_rate_percent": round(hit_rate, 1),
        }

    def clear_cache(self) -> int:
        """Clear all cached PhenoCam data and return count of files removed"""
        cache_files = list(self.cache_dir.glob("*.json"))
        for cache_file in cache_files:
            cache_file.unlink()
        return len(cache_files)
