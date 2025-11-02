#!/usr/bin/env python3
"""
Satellite data query module.
"""

import hashlib
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Optional

import requests

from .config import APIS, COLLECTIONS, SATELLITE
from .spatial_utils import create_buffer_bbox


class SatelliteQuery:
    """Query STAC API for satellite data availability with caching"""

    def __init__(self, cache_dir: str = ".satellite_cache"):
        self.session = requests.Session()
        self.stac_url = APIS["stac"]
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(exist_ok=True)
        self.cache_hits = 0
        self.cache_misses = 0

    def search_satellite_data(
        self,
        lat: float,
        lon: float,
        collection: Optional[str] = None,
        days_back: Optional[int] = None,
        limit: Optional[int] = None,
    ) -> Dict:
        """Search for satellite data at coordinates"""
        if collection is None:
            collection = COLLECTIONS["sentinel2"]
        if days_back is None:
            days_back = SATELLITE["default_days_back"]
        if limit is None:
            limit = SATELLITE["default_limit"]

        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)

        return self.search_satellite_data_daterange(
            lat, lon, start_date, end_date, collection, limit
        )

    def search_satellite_data_daterange(
        self,
        lat: float,
        lon: float,
        start_date: datetime,
        end_date: datetime,
        collection: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> Dict:
        """Search for satellite data at coordinates within date range"""
        if collection is None:
            collection = COLLECTIONS["sentinel2"]
        if limit is None:
            limit = SATELLITE["default_limit"]

        bbox = create_buffer_bbox(lat, lon)

        # Create cache key from query parameters
        cache_key = self._create_cache_key(
            lat, lon, start_date, end_date, collection, limit
        )
        cached_result = self._get_cached_result(cache_key)

        if cached_result is not None:
            import logging

            logger = logging.getLogger(__name__)
            self.cache_hits += 1
            logger.debug(
                f"🗄️  Cache HIT for {collection}: {start_date.date()} to {end_date.date()}"
            )
            return cached_result

        # Essential logging for satellite queries
        import logging

        logger = logging.getLogger(__name__)
        self.cache_misses += 1
        logger.debug(
            f"🌐 API query for {collection}: {start_date.date()} to {end_date.date()}, limit={limit}"
        )

        params = {
            "collections": [collection],
            "datetime": f"{start_date.isoformat()}Z/{end_date.isoformat()}Z",
            "bbox": bbox,
            "limit": limit,
        }

        # Add cloud filter for optical sensors using CQL2 filter
        if "sentinel-2" in collection or "sentinel-3" in collection:
            params["filter"] = {
                "op": "<",
                "args": [
                    {"property": "eo:cloud_cover"},
                    SATELLITE["max_cloud_cover"],
                ],
            }

        response = self.session.post(self.stac_url, json=params)
        response.raise_for_status()

        result = response.json()
        features = result.get("features", [])

        if features:
            feature_dates = [f["properties"]["datetime"] for f in features]
            logger.debug(
                f"Retrieved {len(features)} {collection} features: {min(feature_dates)[:10]} to {max(feature_dates)[:10]}"
            )
        else:
            logger.warning(f"No {collection} features returned for query period")

        # Cache the result
        self._cache_result(cache_key, result)

        return result

    def _create_cache_key(
        self,
        lat: float,
        lon: float,
        start_date: datetime,
        end_date: datetime,
        collection: str,
        limit: int,
    ) -> str:
        """Create a unique cache key for the query parameters"""
        # Round coordinates to avoid cache misses from tiny differences
        lat_rounded = round(lat, 4)
        lon_rounded = round(lon, 4)

        # Create hash from all parameters
        cache_data = {
            "lat": lat_rounded,
            "lon": lon_rounded,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "collection": collection,
            "limit": limit,
            "cloud_filter": SATELLITE["max_cloud_cover"],
        }

        cache_string = json.dumps(cache_data, sort_keys=True)
        return hashlib.md5(cache_string.encode()).hexdigest()

    def _get_cached_result(self, cache_key: str) -> Optional[Dict]:
        """Retrieve cached result if it exists and is valid"""
        cache_file = self.cache_dir / f"{cache_key}.json"

        if not cache_file.exists():
            return None

        try:
            with open(cache_file, "r") as f:
                cached_data = json.load(f)

            # Check if cache is still valid (7 days for historical data)
            cache_timestamp = datetime.fromisoformat(cached_data["timestamp"])
            if (datetime.now() - cache_timestamp).days > 7:
                return None

            return cached_data["result"]

        except (json.JSONDecodeError, KeyError, ValueError):
            # Remove corrupted cache file
            cache_file.unlink(missing_ok=True)
            return None

    def _cache_result(self, cache_key: str, result: Dict) -> None:
        """Cache the API result"""
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
            logger.warning(f"Failed to cache satellite query result: {e}")

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
        """Clear all cached results and return count of files removed"""
        cache_files = list(self.cache_dir.glob("*.json"))
        for cache_file in cache_files:
            cache_file.unlink()
        return len(cache_files)
