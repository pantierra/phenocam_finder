#!/usr/bin/env python3
"""
Core DataFinder class for PhenoCam satellite data discovery.
"""

import json
import statistics
import sys
from datetime import datetime
from typing import Any, Dict, List, Optional

from loguru import logger

from .config import COLLECTIONS, EVALUATION
from .phenocam_query import PhenoCamQuery
from .satellite_query import SatelliteQuery


def _get_int_config(key: str) -> int:
    """Get integer config value with type safety"""
    value = EVALUATION[key]
    if isinstance(value, int):
        return value
    raise TypeError(f"Config {key} expected int, got {type(value)}")


def _get_float_config(key: str) -> float:
    """Get float config value with type safety"""
    value = EVALUATION[key]
    if isinstance(value, (int, float)):
        return float(value)
    raise TypeError(f"Config {key} expected float, got {type(value)}")


def _get_list_int_config(key: str) -> List[int]:
    """Get list of int config value with type safety"""
    value = EVALUATION[key]
    if isinstance(value, list) and all(isinstance(x, int) for x in value):
        return value
    raise TypeError(f"Config {key} expected List[int], got {type(value)}")


class DataFinder:
    """Find satellite data for PhenoCam locations"""

    def __init__(self, cache_dir: str = ".satellite_cache"):
        self.phenocam = PhenoCamQuery(cache_dir=".phenocam_cache")
        self.satellite = SatelliteQuery(cache_dir=cache_dir)

    def get_phenocam_locations(self) -> List[Dict]:
        """Get European PhenoCam locations"""
        return self.phenocam.get_all_locations()

    def search_satellite_data(
        self,
        lat: float,
        lon: float,
        collection: str = "sentinel-2-l2a",
        days_back: int = 365,
        cloud_cover_max: int = 80,
        limit: int = 100,
    ) -> Dict:
        logger.debug(
            f"Searching satellite data for ({lat:.4f}, {lon:.4f}), collection: {collection}, days_back: {days_back}"
        )
        """Search for satellite data at coordinates"""
        return self.satellite.search_satellite_data(
            lat, lon, collection, days_back, limit
        )

    def find_data_for_locations(
        self,
        max_locations: Optional[int] = None,
        collection: str = "sentinel-2-l2a",
    ) -> List[Dict]:
        """Find satellite data for European PhenoCam locations"""
        locations = self.get_phenocam_locations()
        if max_locations:
            locations = locations[:max_locations]

        print(f"Processing {len(locations)} European locations...")

        results = []
        for i, location in enumerate(locations, 1):
            print(f"{i}/{len(locations)}: {location['sitename']}")

            try:
                sat_data = self.search_satellite_data(
                    location["lat"], location["lon"], collection
                )

                result = {
                    "location": location,
                    "satellite_images": len(sat_data.get("features", [])),
                    "latest_image": (
                        sat_data["features"][0]["properties"]["datetime"][:10]
                        if sat_data.get("features")
                        else None
                    ),
                }

                print(f"  Found {result['satellite_images']} images")

            except Exception as e:
                result = {"location": location, "error": str(e)}
                print(f"  Error: {e}")

            results.append(result)

        return results

    def evaluate_site_suitability(
        self,
        lat: float,
        lon: float,
        sitename: str,
        location: Optional[Dict[Any, Any]] = None,
    ) -> Dict:
        """Evaluate site for spatiotemporal fusion suitability"""
        try:
            # Use PhenoCam data availability dates if available
            if location and location.get("date_first") and location.get("date_last"):
                from datetime import datetime

                start_date = datetime.fromisoformat(location["date_first"])
                end_date = datetime.fromisoformat(location["date_last"])
                logger.debug(
                    f"PhenoCam dates for {sitename}: {location['date_first']} to {location['date_last']}"
                )
                logger.debug(
                    f"Parsed satellite query dates: {start_date.isoformat()} to {end_date.isoformat()}"
                )
            else:
                # Fallback to default period
                from datetime import datetime, timedelta

                days_back = _get_int_config("analysis_period_days")
                end_date = datetime.now()
                start_date = end_date - timedelta(days=days_back)
                logger.debug(
                    f"Searching satellite data for {sitename} over {days_back} days (no PhenoCam dates)"
                )

            # Sentinel-2 data (increase limit to get full temporal coverage)
            s2_data = self.satellite.search_satellite_data_daterange(
                lat,
                lon,
                start_date,
                end_date,
                COLLECTIONS["sentinel2"],
                limit=2000,
            )

            # Sentinel-3 data (increase limit to get full temporal coverage)
            s3_data = self.satellite.search_satellite_data_daterange(
                lat,
                lon,
                start_date,
                end_date,
                COLLECTIONS["sentinel3"],
                limit=5000,
            )

            s2_features = s2_data.get("features", []) if s2_data else []
            s3_features = s3_data.get("features", []) if s3_data else []
            logger.debug(
                f"Found {len(s2_features)} S2 features and {len(s3_features)} S3 features for {sitename}"
            )

            # Debug: Show date range of retrieved data
            if s2_features:
                s2_dates = [f["properties"]["datetime"] for f in s2_features]
                logger.debug(
                    f"S2 date range: {min(s2_dates)[:10]} to {max(s2_dates)[:10]}"
                )
            if s3_features:
                s3_dates = [f["properties"]["datetime"] for f in s3_features]
                logger.debug(
                    f"S3 date range: {min(s3_dates)[:10]} to {max(s3_dates)[:10]}"
                )

            # Calculate metrics
            metrics = self._calculate_site_metrics(
                s2_features, s3_features, start_date, end_date
            )
            metrics["sitename"] = sitename
            metrics["coordinates"] = [lat, lon]

            return metrics

        except Exception as e:
            logger.error(f"Error evaluating site {sitename}: {str(e)}")
            return {
                "sitename": sitename,
                "coordinates": [lat, lon],
                "error": str(e),
                "suitability_score": None,  # Unknown suitability due to API error
            }

    def _calculate_site_metrics(
        self,
        s2_features: List,
        s3_features: List,
        start_date=None,
        end_date=None,
    ) -> Dict:
        """Calculate suitability metrics for a site"""
        logger.debug(
            f"Calculating metrics for {len(s2_features)} S2 and {len(s3_features)} S3 features"
        )
        growing_months = _get_list_int_config("growing_season_months")
        logger.debug(f"Growing season months: {growing_months}")

        # Extract dates and filter to growing season
        s2_dates = self._extract_growing_season_dates(s2_features, growing_months)
        s3_dates = self._extract_growing_season_dates(s3_features, growing_months)
        logger.debug(f"Growing season dates - S2: {len(s2_dates)}, S3: {len(s3_dates)}")

        # Debug: Show growing season date ranges
        if s2_dates:
            s2_growing_range = f"{min(s2_dates).date()} to {max(s2_dates).date()}"
            logger.debug(f"S2 growing season range: {s2_growing_range}")
        if s3_dates:
            s3_growing_range = f"{min(s3_dates).date()} to {max(s3_dates).date()}"
            logger.debug(f"S3 growing season range: {s3_growing_range}")

        # Calculate acquisition density
        s2_density = len(s2_dates) / len(growing_months) if growing_months else 0
        s3_density = len(s3_dates) / len(growing_months) if growing_months else 0
        logger.debug(
            f"Scene density - S2: {s2_density:.2f}/month, S3: {s3_density:.2f}/month"
        )

        # Calculate cloud statistics for S2
        s2_clouds = [f["properties"].get("eo:cloud_cover", 100) for f in s2_features]
        s2_cloud_mean = statistics.mean(s2_clouds) if s2_clouds else 100
        logger.debug(
            f"S2 cloud cover - mean: {s2_cloud_mean:.1f}%, samples: {len(s2_clouds)}"
        )

        # Calculate temporal gaps
        s2_gaps = self._calculate_gaps(s2_dates)
        s3_gaps = self._calculate_gaps(s3_dates)

        max_s2_gap = max(s2_gaps) if s2_gaps else 999
        max_s3_gap = max(s3_gaps) if s3_gaps else 999
        logger.debug(f"Max gaps - S2: {max_s2_gap} days, S3: {max_s3_gap} days")

        # Calculate weighted gap scores
        growing_season_days = len(_get_list_int_config("growing_season_months")) * 30
        s2_weighted_gap_score = (
            self._calculate_weighted_gap_score(s2_gaps, growing_season_days)
            if s2_gaps
            else 0.0
        )
        s3_weighted_gap_score = (
            self._calculate_weighted_gap_score(s3_gaps, growing_season_days)
            if s3_gaps
            else 0.0
        )
        s2_gap_count = self._calculate_gap_count(s2_gaps) if s2_gaps else 0

        # Calculate temporal overlap
        overlap_data = self._calculate_temporal_overlap(s2_dates, s3_dates)
        overlap_days = len(overlap_data.get("s2_overlap_dates", []))
        overlap_dates = overlap_data.get("s2_overlap_dates", [])
        logger.debug(f"Temporal overlap: {overlap_days} days")

        # Calculate suitability score
        score = self._calculate_suitability_score(
            s2_density,
            s3_density,
            s2_cloud_mean,
            max_s2_gap,
            overlap_days,
            s2_weighted_gap_score,
            s3_weighted_gap_score,
            s2_gap_count,
        )
        logger.debug(f"Calculated suitability score: {score:.3f}")

        # Add temporal information if available
        temporal_info = {}
        if start_date and end_date:
            temporal_info.update(
                {
                    "phenocam_start_date": start_date.date().isoformat(),
                    "phenocam_end_date": end_date.date().isoformat(),
                    "analysis_period_days": (end_date - start_date).days,
                }
            )

        result = {
            "sentinel2_scenes": len(s2_features),
            "sentinel3_scenes": len(s3_features),
            "s2_scenes_per_month": round(s2_density, 2),
            "s3_scenes_per_month": round(s3_density, 2),
            "cloud_cover_mean": round(s2_cloud_mean, 1),
            "max_s2_gap_days": max_s2_gap,
            "max_s3_gap_days": max_s3_gap,
            "temporal_overlap_days": overlap_days,
            "temporal_overlap_dates": overlap_dates,
            "suitability_score": round(score, 2),
        }
        result.update(temporal_info)
        return result

    def _extract_growing_season_dates(
        self, features: List[Any], growing_months: List[int]
    ) -> List[datetime]:
        """Extract acquisition dates during growing season"""
        dates = []
        for feature in features:
            date_str = feature["properties"]["datetime"]
            date = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
            if date.month in growing_months:
                dates.append(date)
        return sorted(dates)

    def _calculate_gaps(self, dates: List[datetime]) -> List[int]:
        """Calculate gaps between consecutive dates"""
        if len(dates) < 2:
            return []
        return [(dates[i + 1] - dates[i]).days for i in range(len(dates) - 1)]

    def _calculate_gap_count(
        self, gaps: List[int], threshold: Optional[int] = None
    ) -> int:
        """Count gaps exceeding threshold days

        Args:
            gaps: List of gap durations in days
            threshold: Days threshold for counting gaps (uses config if None)
        """
        if not gaps:
            return 0
        if threshold is None:
            threshold = _get_int_config("gap_count_threshold")
        return sum(1 for gap in gaps if gap > threshold)

    def _calculate_weighted_gap_score(
        self,
        gaps: List[int],
        season_length: int = 214,
        tau: Optional[int] = None,
    ) -> float:
        """Calculate exponential weighted gap score (WGS_exp)

        Algorithm-specific alignment with EFAST using exponential temporal decay weighting.
        Captures both gap frequency and duration with non-linear penalization.

        Formula: WGS_exp = (1/T) * Σ[exp(Δt_i/τ) * Δt_i]
        where T = season length, Δt_i = gap duration, τ = decay parameter

        For cross-site comparison, normalize as:
        WGS_norm = (WGS_exp - WGS_min) / (WGS_max - WGS_min)

        Args:
            gaps: List of gap durations in days
            season_length: Growing season length in days (default 214 for Apr-Oct)
            tau: Exponential decay parameter (uses config if None, EFAST temporal window)

        Returns:
            Weighted gap score normalized by season length
        """
        if not gaps:
            return 0.0

        if tau is None:
            tau = _get_int_config("weighted_gap_tau")

        import math

        # Calculate exponential weights and weighted sum (numpy-style logic)
        weights = [math.exp(gap / tau) for gap in gaps]
        weighted_gaps = [weight * gap for weight, gap in zip(weights, gaps)]
        wgs = sum(weighted_gaps) / season_length
        return wgs

    def _calculate_temporal_overlap(
        self, s2_dates: List[datetime], s3_dates: List[datetime]
    ) -> Dict:
        """Calculate temporal overlap dates within ±3 days for both sensors"""
        if not s2_dates or not s3_dates:
            result: Dict[str, Any] = {}
            if EVALUATION["long_output"]:
                result["s2_overlap_dates"] = []
                result["s3_overlap_dates"] = []
            return result

        s2_overlap_dates = []
        s3_overlap_dates = []

        if EVALUATION["long_output"]:
            # Find S2 dates that have matching S3 within ±3 days
            for s2_date in s2_dates:
                for s3_date in s3_dates:
                    if abs((s2_date - s3_date).days) <= 3:
                        s2_overlap_dates.append(s2_date.date().isoformat())
                        break

            # Find S3 dates that have matching S2 within ±3 days
            for s3_date in s3_dates:
                for s2_date in s2_dates:
                    if abs((s3_date - s2_date).days) <= 3:
                        s3_overlap_dates.append(s3_date.date().isoformat())
                        break

        result = {}
        if EVALUATION["long_output"]:
            result["s2_overlap_dates"] = s2_overlap_dates
            result["s3_overlap_dates"] = s3_overlap_dates
        return result

    def _calculate_suitability_score(
        self,
        s2_density: float,
        s3_density: float,
        cloud_mean: float,
        max_gap: int,
        overlap_days: int,
        s2_weighted_gap_score: float = 0.0,
        s3_weighted_gap_score: float = 0.0,
        gap_count: int = 0,
    ) -> float:
        """Calculate enhanced suitability score (0-1) using simplified mathematical functions.

        Uses balanced multi-component weighted scoring treating S2 and S3 equally:

        - S2 Scene Density (30%): Linear scaling with saturation at 3 scenes/month
        - S3 Scene Density (30%): Linear scaling with saturation at 2 scenes/month
        - Cloud Cover (25%): Exponential decay penalty (cloud/80)^1.5
        - Gap Analysis (15%): Combined assessment using mathematical functions:
          * Max gap: Exponential decay (gap/30)^1.2
          * Weighted gap severity: Linear penalty based on pre-calculated scores
          * Gap frequency: Relative to expected observations

        Mathematical approach eliminates complex if-elif chains while maintaining
        smooth scoring curves that properly discriminate between site qualities.

        Returns scores typically ranging 0.15-0.98, with excellent sites scoring >0.9
        and poor sites scoring <0.4.
        """
        # Handle edge case: no data at all
        if s2_density == 0:
            return 0.0

        # S2 Density score (30%) - Sigmoid-like curve with saturation at 3 scenes/month
        s2_density_score = min(s2_density / 3.0, 1.0) * 0.3

        # S3 Density score (30%) - Sigmoid-like curve with saturation at 2 scenes/month
        s3_density_score = min(s3_density / 2.0, 1.0) * 0.3

        # Cloud score (25%) - Exponential decay penalty for increasing cloud cover
        cloud_factor = max(0.05, 1.0 - (cloud_mean / 80.0) ** 1.5)
        cloud_score = cloud_factor * 0.25

        # Max gap component - Exponential decay for increasing gap days
        max_gap_component = max(0.1, 1.0 - (max_gap / 30.0) ** 1.2)

        # Weighted gap component - Penalty for high weighted gap scores
        weighted_gap_component = max(0.1, 1.0 - min(s2_weighted_gap_score / 2.0, 1.0))

        # Gap frequency component - Penalty based on gap count relative to expected scenes
        expected_obs = max(s2_density * 8, 1)  # ~8 months growing season
        gap_freq_penalty = min(gap_count / expected_obs, 1.0) if gap_count > 0 else 0.0
        gap_freq_component = max(0.2, 1.0 - gap_freq_penalty)

        # Combined gap score (15%) with balanced weighting
        gap_score = (
            max_gap_component * 0.4
            + weighted_gap_component * 0.4
            + gap_freq_component * 0.2
        ) * 0.15

        return s2_density_score + s3_density_score + cloud_score + gap_score

    def evaluate_all_sites(self, max_locations: Optional[int] = None) -> List[Dict]:
        """Evaluate all European sites for suitability by growing season"""
        locations = self.get_phenocam_locations()
        if max_locations:
            locations = locations[:max_locations]
            logger.info(f"Limited evaluation to first {max_locations} locations")

        print(
            f"Evaluating {len(locations)} European PhenoCam sites by growing season..."
        )
        logger.info(f"Starting seasonal evaluation of {len(locations)} sites")

        results = []
        for i, location in enumerate(locations, 1):
            sitename = location["sitename"]
            print(f"{i}/{len(locations)}: {sitename}")
            logger.debug(
                f"Evaluating site {i}/{len(locations)}: {sitename} at ({location['lat']:.4f}, {location['lon']:.4f})"
            )

            # Generate seasonal results for each location
            seasonal_results = self.evaluate_site_by_seasons(
                location["lat"], location["lon"], sitename, location
            )

            results.extend(seasonal_results)

        return results

    def evaluate_site_by_seasons(
        self,
        lat: float,
        lon: float,
        sitename: str,
        location: Optional[Dict[Any, Any]] = None,
    ) -> List[Dict]:
        """Evaluate site for each growing season with buffer months"""
        from datetime import datetime

        # Get available years from PhenoCam dates
        if location and location.get("date_first") and location.get("date_last"):
            start_date = datetime.fromisoformat(location["date_first"])
            end_date = datetime.fromisoformat(location["date_last"])
        else:
            # Fallback to current year
            end_date = datetime.now()
            start_date = datetime(end_date.year, 1, 1)

        # Get all years in the date range
        years = list(range(start_date.year, end_date.year + 1))

        seasonal_results = []
        growing_months = _get_list_int_config("growing_season_months")

        # Add buffer month before and after growing season
        buffer_months = []
        if growing_months:
            min_month = min(growing_months)
            max_month = max(growing_months)
            if min_month > 1:
                buffer_months.append(min_month - 1)
            if max_month < 12:
                buffer_months.append(max_month + 1)

        season_months = sorted(set(growing_months + buffer_months))

        for year in years:
            season_start = datetime(year, min(season_months), 1)
            season_end = datetime(
                year, max(season_months), 28
            )  # Safe day for all months

            try:
                # Query satellite data for this growing season
                s2_data = (
                    self.satellite.search_satellite_data_daterange(
                        lat,
                        lon,
                        season_start,
                        season_end,
                        COLLECTIONS["sentinel2"],
                        1000,
                    )
                    if self.satellite
                    else None
                )
                s3_data = (
                    self.satellite.search_satellite_data_daterange(
                        lat,
                        lon,
                        season_start,
                        season_end,
                        COLLECTIONS["sentinel3"],
                        1000,
                    )
                    if self.satellite
                    else None
                )

                # Calculate metrics for this season
                result = self._calculate_seasonal_metrics(
                    s2_data or {},
                    s3_data or {},
                    sitename,
                    year,
                    season_start,
                    season_end,
                    season_months,
                )

                # Add location metadata
                result["vegetation_type"] = (
                    location.get("vegetation_type", "") if location else ""
                )
                result["description"] = (
                    location.get("description", "") if location else ""
                )
                result["lat"] = lat
                result["lon"] = lon
                result["growing_season_year"] = year

                seasonal_results.append(result)
                logger.debug(
                    f"Seasonal evaluation for {sitename} {year}: score {result.get('suitability_score', 'N/A')}"
                )

            except Exception as e:
                logger.warning(f"Failed to evaluate {sitename} for {year}: {str(e)}")
                seasonal_results.append(
                    {
                        "sitename": sitename,
                        "lat": lat,
                        "lon": lon,
                        "growing_season_year": year,
                        "vegetation_type": location.get("vegetation_type", "")
                        if location
                        else "",
                        "description": location.get("description", "")
                        if location
                        else "",
                        "error": str(e),
                        "suitability_score": None,  # Unknown due to error
                    }
                )

        return seasonal_results

    def _calculate_seasonal_metrics(
        self,
        s2_data: Dict,
        s3_data: Dict,
        sitename: str,
        year: int,
        season_start: datetime,
        season_end: datetime,
        season_months: List[int],
    ) -> Dict:
        """Calculate metrics for a specific growing season"""
        s2_features = s2_data.get("features", []) if s2_data else []
        s3_features = s3_data.get("features", []) if s3_data else []

        # Filter dates to season months only
        s2_dates = self._extract_growing_season_dates(s2_features, season_months)
        s3_dates = self._extract_growing_season_dates(s3_features, season_months)

        # Calculate basic metrics
        result = {
            "sitename": sitename,
            "sentinel2_scenes": len(s2_dates),
            "sentinel3_scenes": len(s3_dates),
            "season_start_date": season_start.isoformat()[:10],
            "season_end_date": season_end.isoformat()[:10],
            "season_length_days": (season_end - season_start).days,
        }

        if not s2_dates and not s3_dates:
            result["error"] = "No satellite data available for this season"
            result["suitability_score"] = 0.0
            return result

        # Calculate season length for gap metrics
        season_length = (season_end - season_start).days

        # Temporal metrics
        if s2_dates:
            s2_gaps = self._calculate_gaps(s2_dates)
            result["max_s2_gap_days"] = max(s2_gaps) if s2_gaps else 0
            result["s2_gap_count"] = self._calculate_gap_count(s2_gaps)
            result["s2_weighted_gap_score"] = round(
                self._calculate_weighted_gap_score(s2_gaps, season_length), 3
            )
            result["s2_first_date"] = s2_dates[0].isoformat()[:10]
            result["s2_last_date"] = s2_dates[-1].isoformat()[:10]
        else:
            result["max_s2_gap_days"] = None
            result["s2_gap_count"] = 0
            result["s2_weighted_gap_score"] = 0.0

        if s3_dates:
            s3_gaps = self._calculate_gaps(s3_dates)
            result["max_s3_gap_days"] = max(s3_gaps) if s3_gaps else 0
            result["s3_gap_count"] = self._calculate_gap_count(s3_gaps)
            result["s3_weighted_gap_score"] = round(
                self._calculate_weighted_gap_score(s3_gaps, season_length), 3
            )
            result["s3_first_date"] = s3_dates[0].isoformat()[:10]
            result["s3_last_date"] = s3_dates[-1].isoformat()[:10]
        else:
            result["max_s3_gap_days"] = None
            result["s3_gap_count"] = 0
            result["s3_weighted_gap_score"] = 0.0

        # Cloud cover analysis for S2
        if s2_features:
            s2_cloud_covers = [
                f["properties"].get("eo:cloud_cover", 0) for f in s2_features
            ]
            result["s2_cloud_cover_mean"] = round(statistics.mean(s2_cloud_covers), 1)
            result["s2_cloud_cover_std"] = round(
                (statistics.stdev(s2_cloud_covers) if len(s2_cloud_covers) > 1 else 0),
                1,
            )
        else:
            result["s2_cloud_cover_mean"] = 0
            result["s2_cloud_cover_std"] = 0

        # Cloud cover analysis for S3
        if s3_features:
            s3_cloud_covers = [
                f["properties"].get("eo:cloud_cover", 0) for f in s3_features
            ]
            result["s3_cloud_cover_mean"] = round(statistics.mean(s3_cloud_covers), 1)
            result["s3_cloud_cover_std"] = round(
                (statistics.stdev(s3_cloud_covers) if len(s3_cloud_covers) > 1 else 0),
                1,
            )
        else:
            result["s3_cloud_cover_mean"] = 0
            result["s3_cloud_cover_std"] = 0

        # Temporal overlap
        overlap_data = self._calculate_temporal_overlap(s2_dates, s3_dates)
        result.update(overlap_data)

        # Calculate scene densities
        season_days_value = result["season_length_days"]
        season_days = (
            int(season_days_value) if isinstance(season_days_value, (int, float)) else 0
        )
        s2_density = (len(s2_dates) / season_days) * 30 if season_days > 0 else 0
        s3_density = (len(s3_dates) / season_days) * 30 if season_days > 0 else 0

        # Calculate suitability score for this season
        # Calculate overlap count for scoring
        overlap_count = len(overlap_data.get("s2_overlap_dates", []))

        s2_cloud_mean = result.get("s2_cloud_cover_mean", 0.0)
        s2_gap = result.get("max_s2_gap_days", 999)
        s3_gap = result.get("max_s3_gap_days", 999)

        # Ensure proper types
        if not isinstance(s2_cloud_mean, (int, float)):
            s2_cloud_mean = 0.0
        if not isinstance(s2_gap, (int, float)):
            s2_gap = 999
        if not isinstance(s3_gap, (int, float)):
            s3_gap = 999

        # Extract values with proper typing for mypy
        s2_weighted_score = result.get("s2_weighted_gap_score", 0.0)
        s3_weighted_score = result.get("s3_weighted_gap_score", 0.0)
        s2_gaps_count = result.get("s2_gap_count", 0)

        # Type assertions for mypy
        assert isinstance(s2_weighted_score, (int, float))
        assert isinstance(s3_weighted_score, (int, float))
        assert isinstance(s2_gaps_count, (int, float))

        result["suitability_score"] = round(
            self._calculate_suitability_score(
                s2_density,
                s3_density,
                float(s2_cloud_mean),
                int(min(s2_gap, s3_gap)),
                overlap_count,
                float(s2_weighted_score),
                float(s3_weighted_score),
                int(s2_gaps_count),
            ),
            2,
        )

        return result

    def export_geojson(
        self, results: List[Dict], filename: str = "site_analysis.geojson"
    ):
        """Export site analysis results to GeoJSON grouped by location"""
        if not results:
            return

        # Group results by sitename
        grouped_results = {}
        for result in results:
            if "lat" not in result or "lon" not in result:
                continue

            sitename = result.get("sitename", "")
            if sitename not in grouped_results:
                grouped_results[sitename] = {
                    "lat": result["lat"],
                    "lon": result["lon"],
                    "vegetation_type": result.get("vegetation_type", ""),
                    "description": result.get("description", ""),
                    "seasons": {},
                }

            # Add seasonal data
            year = result.get("growing_season_year", "")
            if year and not result.get("error"):
                season_data = {
                    "sentinel2_scenes": result.get("sentinel2_scenes", ""),
                    "sentinel3_scenes": result.get("sentinel3_scenes", ""),
                    "s2_cloud_cover_mean": result.get("s2_cloud_cover_mean", ""),
                    "s3_cloud_cover_mean": result.get("s3_cloud_cover_mean", ""),
                    "max_s2_gap_days": result.get("max_s2_gap_days", ""),
                    "max_s3_gap_days": result.get("max_s3_gap_days", ""),
                    "s2_gap_count": result.get("s2_gap_count", ""),
                    "s3_gap_count": result.get("s3_gap_count", ""),
                    "s2_weighted_gap_score": result.get("s2_weighted_gap_score", ""),
                    "s3_weighted_gap_score": result.get("s3_weighted_gap_score", ""),
                    "season_start_date": result.get("season_start_date", ""),
                    "season_end_date": result.get("season_end_date", ""),
                    "season_length_days": result.get("season_length_days", ""),
                    "suitability_score": result.get("suitability_score", ""),
                }

                # Add detailed temporal data only if long_output is enabled
                if EVALUATION["long_output"]:
                    season_data["s2_cloud_cover_std"] = result.get(
                        "s2_cloud_cover_std", ""
                    )
                    season_data["s3_cloud_cover_std"] = result.get(
                        "s3_cloud_cover_std", ""
                    )
                    season_data["s2_first_date"] = result.get("s2_first_date", "")
                    season_data["s2_last_date"] = result.get("s2_last_date", "")
                    season_data["s3_first_date"] = result.get("s3_first_date", "")
                    season_data["s3_last_date"] = result.get("s3_last_date", "")
                    season_data["s2_overlap_dates"] = result.get("s2_overlap_dates", [])
                    season_data["s3_overlap_dates"] = result.get("s3_overlap_dates", [])

                grouped_results[sitename]["seasons"][str(year)] = season_data

        # Create GeoJSON structure
        geojson: Dict[str, Any] = {"type": "FeatureCollection", "features": []}

        for sitename, site_data in grouped_results.items():
            properties = {
                "sitename": sitename,
                "vegetation_type": site_data["vegetation_type"],
                "description": site_data["description"],
                "seasons": site_data["seasons"],
            }

            # Create GeoJSON feature
            feature = {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [
                        site_data["lon"],
                        site_data["lat"],
                    ],  # GeoJSON uses lon, lat
                },
                "properties": properties,
            }

            geojson["features"].append(feature)

        # Write to file
        with open(filename, "w") as f:
            json.dump(geojson, f, indent=2)

        logger.info(f"GeoJSON results saved to {filename}")


def main():
    """Main function for command line usage"""
    # Configure logger for debug output
    logger.remove()  # Remove default handler
    logger.add(
        sys.stderr,
        level="DEBUG",
        format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
    )
    logger.info("=== Starting PhenoCam Satellite Data Finder ===")

    finder = DataFinder()

    # Get number of locations to process from command line
    max_locations = int(sys.argv[1]) if len(sys.argv) > 1 else 1

    # Evaluate sites for suitability
    results = finder.evaluate_all_sites(max_locations)

    # Print top sites summary
    # Show summary
    successful_results = [r for r in results if "error" not in r]
    logger.info(
        f"Evaluation completed: {len(results)} sites processed, {len(successful_results)} successful"
    )
    print("\n=== Site Evaluation Summary ===")
    print(f"Sites evaluated: {len(results)}")
    print(f"Successful evaluations: {len(successful_results)}")

    if successful_results:
        print("\nTop 5 Sites:")
        for i, site in enumerate(successful_results[:5], 1):
            print(
                f"{i}. {site['sitename']} (Score: {site.get('suitability_score', 'N/A')})"
            )
            print(
                f"   S2: {site['sentinel2_scenes']}, S3: {site['sentinel3_scenes']}, S2 Cloud: {site.get('s2_cloud_cover_mean', 0):.1f}%"
            )

    # Show cache statistics
    sat_cache_stats = finder.satellite.get_cache_stats()
    phenocam_cache_stats = finder.phenocam.get_cache_stats()

    total_hits = sat_cache_stats["cache_hits"] + phenocam_cache_stats["cache_hits"]
    total_misses = (
        sat_cache_stats["cache_misses"] + phenocam_cache_stats["cache_misses"]
    )
    total_requests = total_hits + total_misses

    if total_requests > 0:
        overall_hit_rate = (
            (total_hits / total_requests * 100) if total_requests > 0 else 0
        )
        logger.info(
            f"Cache performance: {total_hits} hits, {total_misses} misses "
            f"({overall_hit_rate:.1f}% hit rate) - "
            f"Satellite: {sat_cache_stats['hit_rate_percent']}%, "
            f"PhenoCam: {phenocam_cache_stats['hit_rate_percent']}%"
        )

    # Export results
    logger.info("Exporting results to files")
    finder.export_geojson(results, "site_evaluation_results.geojson")
    logger.info("=== PhenoCam Satellite Data Finder completed ===")
