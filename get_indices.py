#!/usr/bin/env python3
"""
Calculate NDVI time series and statistics from Sentinel-2 imagery.
"""

import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Dict, List, Tuple

import ee
import yaml  # type: ignore


def init_ee():
    """Initialize Earth Engine."""
    try:
        ee.Initialize(project="fluent-optics-344414")
    except Exception:
        ee.Authenticate()
        ee.Initialize(project="fluent-optics-344414")


def load_config() -> Dict:
    """Load configuration from YAML."""
    import yaml

    with open("config.yaml", "r") as f:
        return yaml.safe_load(f)


def load_all_sites() -> Dict:
    """Load all sites from GeoJSON."""
    with open("all_sites.geojson", "r") as f:
        return json.load(f)


def calculate_gaps(dates: List[str], threshold_days: int = 5) -> Tuple[int, int, float]:
    """Calculate gap statistics for scene dates."""
    if len(dates) < 2:
        return 0, 0, 0.0

    # Convert string dates to datetime objects and sort
    unique_dates = sorted(set(datetime.fromisoformat(date) for date in dates))

    if len(unique_dates) < 2:
        return 0, 0, 0.0

    gaps = [
        (unique_dates[i] - unique_dates[i - 1]).days
        for i in range(1, len(unique_dates))
    ]
    gaps = [g for g in gaps if g > threshold_days]

    return (
        max(gaps, default=0),
        len(gaps),
        sum(g * g for g in gaps) / (len(unique_dates) - 1) if gaps else 0.0,
    )


def calculate_ndvi(lat: float, lon: float, start_date: str, end_date: str) -> Dict:
    """Calculate NDVI time series for all S2 dates, with null for cloudy/invalid."""
    # Load NDVI thresholds from config
    config = load_config()
    ndvi_min = config.get("ndvi_min_threshold", 0.1)
    ndvi_max = config.get("ndvi_max_threshold", 0.95)

    point = ee.Geometry.Point([lon, lat])
    region = point.buffer(100)

    from collections import defaultdict

    # Get ALL Sentinel-2 scenes (no cloud filter)
    all_collection = (
        ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")
        .filterBounds(region)
        .filterDate(start_date, end_date)
    )

    # Get all unique scene dates
    all_dates = (
        all_collection.map(
            lambda img: ee.Feature(None, {"date": img.date().format("YYYY-MM-dd")})
        )
        .distinct("date")
        .getInfo()["features"]
    )

    scene_dates = sorted(set(f["properties"]["date"] for f in all_dates))

    # Calculate NDVI only for clear scenes with cloud masking
    def mask_clouds(image):
        """Apply cloud mask using QA60 band."""
        qa = image.select("QA60")
        cloud_mask = qa.bitwiseAnd(1 << 10).eq(0).And(qa.bitwiseAnd(1 << 11).eq(0))
        return image.updateMask(cloud_mask)

    def add_ndvi(image):
        """Add NDVI band to image."""
        ndvi = image.normalizedDifference(["B8", "B4"]).rename("NDVI")
        return image.addBands(ndvi).set("date", image.date().format("YYYY-MM-dd"))

    # Process ALL scenes and calculate NDVI
    ndvi_collection = all_collection.map(mask_clouds).map(add_ndvi)

    # Extract NDVI values at point
    def extract_ndvi(image):
        """Extract NDVI value at point."""
        ndvi_value = image.select("NDVI").reduceRegion(
            reducer=ee.Reducer.mean(),
            geometry=region,
            scale=10,
            maxPixels=1e9,
        )
        return ee.Feature(
            None,
            {
                "date": image.get("date"),
                "ndvi": ndvi_value.get("NDVI"),
            },
        )

    time_series = ndvi_collection.map(extract_ndvi)
    ts_list = time_series.getInfo()["features"]

    # Collect NDVI values by date
    date_ndvi = defaultdict(list)
    used_dates = []  # Track dates with plausible NDVI for stats
    for feature in ts_list:
        ndvi_val = feature["properties"].get("ndvi")
        if ndvi_val is not None:
            date = feature["properties"]["date"]
            date_ndvi[date].append(ndvi_val)

    # Build complete time series marking outliers
    ndvi_series = []
    ndvi_values = []  # Only plausible NDVI values for statistics

    for date in scene_dates:
        if date in date_ndvi:
            # NDVI value available - check if plausible
            values = date_ndvi[date]
            avg_ndvi = sum(values) / len(values)

            # Check if NDVI is plausible
            is_outlier = avg_ndvi < ndvi_min or avg_ndvi > ndvi_max

            ndvi_series.append(
                {
                    "date": date,
                    "ndvi": round(avg_ndvi, 4),
                    "outlier": is_outlier,  # Mark outliers
                }
            )

            # Only include plausible values in stats
            if not is_outlier:
                ndvi_values.append(avg_ndvi)
                used_dates.append(date)
        else:
            # No NDVI data available
            ndvi_series.append(
                {
                    "date": date,
                    "ndvi": None,  # Will be displayed as N/A
                }
            )

    # Calculate gap statistics ONLY for dates with plausible NDVI values
    # Excludes outliers (likely clouds/artifacts at the specific location)
    # This differs from get_scenes.py which calculates gaps for ALL available scenes
    gap_stats = calculate_gaps(used_dates, threshold_days=3)

    # Calculate statistics (only from valid values)
    stats = {
        "ndvi_time_series": ndvi_series,  # All dates, null for cloudy
        "ndvi_observations": len(ndvi_values),  # Count of valid NDVI only
        "ndvi_mean": round(sum(ndvi_values) / len(ndvi_values), 4)
        if ndvi_values
        else 0.0,
        "ndvi_min": round(min(ndvi_values), 4) if ndvi_values else 0.0,
        "ndvi_max": round(max(ndvi_values), 4) if ndvi_values else 0.0,
        "ndvi_max_s2_gap_days": gap_stats[0],  # Gap stats for NDVI-used scenes only
        "ndvi_s2_gap_count": gap_stats[1],  # Number of gaps in NDVI-used scenes
        "ndvi_s2_weighted_gap_score": round(
            gap_stats[2], 2
        ),  # Weighted gap for NDVI scenes
    }

    # Add range
    if ndvi_values:
        stats["ndvi_range"] = round(stats["ndvi_max"] - stats["ndvi_min"], 4)
    else:
        stats["ndvi_range"] = 0.0

    return stats


def process_site_season(
    site_id: str,
    year: str,
    lat: float,
    lon: float,
    start_date: str,
    end_date: str,
) -> Tuple[str, str, Dict]:
    """Process NDVI for a single site-season combination."""
    # Initialize EE for this thread
    ee.Initialize(project="fluent-optics-344414")

    try:
        ndvi_stats = calculate_ndvi(lat, lon, start_date, end_date)
        return site_id, year, ndvi_stats
    except Exception as e:
        raise Exception(f"Error calculating NDVI for {site_id} {year}: {e}")


def process_ndvi(data):
    """Calculate NDVI for selected sites and seasons from config with parallel processing."""
    # Load config to get selected seasons
    with open("config.yaml", "r") as f:
        config = yaml.safe_load(f)

    sites_config = config.get("sites", {})

    # Build list of tasks to process
    tasks = []
    for site_id, site_data in data["sites"].items():
        selected_years = sites_config.get(site_id, [])
        if not selected_years:
            continue

        lat = site_data["lat"]
        lon = site_data["lon"]

        for year, season in site_data["seasons"].items():
            if int(year) not in selected_years and year not in [
                str(y) for y in selected_years
            ]:
                continue

            start_date = season.get("season_start_date")
            end_date = season.get("season_end_date")

            if start_date and end_date:
                tasks.append((site_id, year, lat, lon, start_date, end_date))

    if not tasks:
        print("No sites/seasons selected for NDVI processing")
        return data

    print(f"Processing NDVI for {len(tasks)} site-seasons using parallel execution")
    print("Using 5 parallel threads\n")

    completed = 0
    with ThreadPoolExecutor(max_workers=5) as executor:
        # Submit all tasks
        futures = {
            executor.submit(
                process_site_season,
                site_id,
                year,
                lat,
                lon,
                start_date,
                end_date,
            ): (site_id, year)
            for site_id, year, lat, lon, start_date, end_date in tasks
        }

        # Process completed futures
        for future in as_completed(futures):
            site_id, year = futures[future]
            try:
                processed_site, processed_year, ndvi_stats = future.result()

                # Update the data
                season = data["sites"][processed_site]["seasons"][processed_year]
                season["ndvi_mean"] = ndvi_stats["ndvi_mean"]
                season["ndvi_min"] = ndvi_stats["ndvi_min"]
                season["ndvi_max"] = ndvi_stats["ndvi_max"]
                season["ndvi_range"] = ndvi_stats["ndvi_range"]
                season["ndvi_observations"] = ndvi_stats["ndvi_observations"]
                season["ndvi_time_series"] = ndvi_stats["ndvi_time_series"]
                season["ndvi_max_s2_gap_days"] = ndvi_stats["ndvi_max_s2_gap_days"]
                season["ndvi_s2_gap_count"] = ndvi_stats["ndvi_s2_gap_count"]
                season["ndvi_s2_weighted_gap_score"] = ndvi_stats[
                    "ndvi_s2_weighted_gap_score"
                ]

                completed += 1
                print(
                    f"[{completed}/{len(tasks)}] {processed_site} {processed_year}: "
                    f"{ndvi_stats['ndvi_observations']} obs, "
                    f"mean={ndvi_stats['ndvi_mean']:.3f}"
                )
            except Exception as e:
                completed += 1
                print(f"[{completed}/{len(tasks)}] {site_id} {year}: Failed - {e}")
                # Keep default values
                data["sites"][site_id]["seasons"][year]["ndvi_range"] = 0.0

    return data


def save_selected_sites(geojson: Dict):
    """Save selected sites with NDVI data to GeoJSON."""
    with open("selected_sites.geojson", "w") as f:
        json.dump(geojson, f, indent=2, default=str)
    print("Updated selected_sites.geojson with NDVI statistics")


def main():
    """Main entry point."""
    init_ee()

    # Load config and all sites
    config = load_config()
    all_sites_geojson = load_all_sites()

    # Build sites data structure from GeoJSON for selected sites only
    sites = {}
    selected_features = []

    for feature in all_sites_geojson["features"]:
        props = feature["properties"]
        sitename = props["sitename"]

        # Check if this site is in config
        if sitename in config["sites"]:
            selected_years = config["sites"][sitename]

            # Filter seasons to only include selected years
            selected_seasons = {}
            for year_str in selected_years:
                year = str(year_str)
                if year in props["seasons"]:
                    selected_seasons[year] = props["seasons"][year]

            if selected_seasons:
                # Create site entry
                sites[sitename] = {
                    "sitename": sitename,
                    "lat": feature["geometry"]["coordinates"][1],
                    "lon": feature["geometry"]["coordinates"][0],
                    "vegetation_type": props["vegetation_type"],
                    "description": props["description"],
                    "elevation": props["elevation"],
                    "country": props.get("country", ""),
                    "ndvi_selected": True,
                    "seasons": selected_seasons,
                }

                # Keep feature for output
                feature_copy = json.loads(json.dumps(feature))
                feature_copy["properties"]["seasons"] = selected_seasons
                selected_features.append(feature_copy)

    # Process NDVI for selected sites
    data = {"sites": sites}
    data = process_ndvi(data)

    # Update features with NDVI results
    for feature in selected_features:
        sitename = feature["properties"]["sitename"]
        if sitename in data["sites"]:
            site_data = data["sites"][sitename]
            for year, season in site_data["seasons"].items():
                if year in feature["properties"]["seasons"]:
                    feature["properties"]["seasons"][year].update(
                        {
                            "ndvi_mean": season.get("ndvi_mean", 0.0),
                            "ndvi_min": season.get("ndvi_min", 0.0),
                            "ndvi_max": season.get("ndvi_max", 0.0),
                            "ndvi_range": season.get("ndvi_range", 0.0),
                            "ndvi_observations": season.get("ndvi_observations", 0),
                            "ndvi_time_series": season.get("ndvi_time_series", []),
                            "ndvi_max_s2_gap_days": season.get(
                                "ndvi_max_s2_gap_days", 0
                            ),
                            "ndvi_s2_gap_count": season.get("ndvi_s2_gap_count", 0),
                            "ndvi_s2_weighted_gap_score": season.get(
                                "ndvi_s2_weighted_gap_score", 0.0
                            ),
                        }
                    )

    # Save selected sites with NDVI data
    selected_geojson = {
        "type": "FeatureCollection",
        "features": selected_features,
    }
    save_selected_sites(selected_geojson)


if __name__ == "__main__":
    main()
