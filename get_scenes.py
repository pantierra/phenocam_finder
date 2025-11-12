#!/usr/bin/env python3
"""
Fetch satellite scene statistics using Google Earth Engine with parallel processing.
"""

import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Dict, List, Tuple

import ee


def init_ee():
    """Initialize Earth Engine."""
    try:
        ee.Initialize(project="fluent-optics-344414")
    except Exception:
        ee.Authenticate()
        ee.Initialize(project="fluent-optics-344414")


def get_s2_scenes(
    point: ee.Geometry.Point, start_date: str, end_date: str
) -> Tuple[List[Dict], float]:
    """Get Sentinel-2 scenes and cloud statistics (unique dates only)."""
    collection = (
        ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")
        .filterBounds(point.buffer(100))
        .filterDate(start_date, end_date)
    )

    scenes = collection.map(
        lambda img: ee.Feature(
            None,
            {
                "date": img.date().format("YYYY-MM-dd"),
                "cloud": img.get("CLOUDY_PIXEL_PERCENTAGE"),
            },
        )
    ).getInfo()["features"]

    # Deduplicate by date, averaging cloud coverage
    from collections import defaultdict

    date_scenes = defaultdict(list)
    for s in scenes:
        date = s["properties"]["date"]
        cloud = s["properties"]["cloud"]
        date_scenes[date].append(cloud)

    # Create unique scenes with averaged cloud coverage
    unique_scenes = []
    for date, clouds in date_scenes.items():
        unique_scenes.append(
            {"properties": {"date": date, "cloud": sum(clouds) / len(clouds)}}
        )

    cloud_mean = (
        sum(s["properties"]["cloud"] for s in unique_scenes) / len(unique_scenes)
        if unique_scenes
        else 0.0
    )
    return unique_scenes, cloud_mean


def get_s3_scenes(
    point: ee.Geometry.Point, start_date: str, end_date: str
) -> List[Dict]:
    """Get Sentinel-3 OLCI scenes (unique dates only)."""
    collection = (
        ee.ImageCollection("COPERNICUS/S3/OLCI")
        .filterBounds(point.buffer(1000))
        .filterDate(start_date, end_date)
    )

    # Get scene dates
    scenes = collection.map(
        lambda img: ee.Feature(
            None,
            {
                "date": img.date().format("YYYY-MM-dd"),
            },
        )
    ).getInfo()["features"]

    # Deduplicate by date
    unique_dates = list(set(s["properties"]["date"] for s in scenes))
    unique_scenes = [{"properties": {"date": date}} for date in unique_dates]

    return unique_scenes


def calculate_gaps(
    scenes: List[Dict], threshold_days: int = 5
) -> Tuple[int, int, float]:
    """Calculate gap statistics for scenes."""
    if len(scenes) < 2:
        return 0, 0, 0.0

    # Get unique dates only (multiple scenes can occur on same day)
    unique_dates = sorted(
        set(datetime.fromisoformat(s["properties"]["date"]) for s in scenes)
    )

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


def process_season(lat: float, lon: float, season: Dict) -> Dict:
    """Process a single season's satellite data."""
    start_date = season.get("season_start_date")
    end_date = season.get("season_end_date")

    if not (start_date and end_date):
        return season

    point = ee.Geometry.Point([lon, lat])

    # Sentinel-2
    try:
        s2_scenes, s2_cloud = get_s2_scenes(point, start_date, end_date)
        s2_gaps = calculate_gaps(s2_scenes, threshold_days=3)

        season.update(
            {
                "sentinel2_scenes": len(s2_scenes),
                "s2_cloud_cover_mean": round(s2_cloud, 2),
                "max_s2_gap_days": s2_gaps[0],
                "s2_gap_count": s2_gaps[1],
                "s2_weighted_gap_score": round(s2_gaps[2], 2),
            }
        )
    except Exception as e:
        print(f"    S2 error: {e}")

    # Sentinel-3 - only get scene count
    try:
        s3_scenes = get_s3_scenes(point, start_date, end_date)

        season.update(
            {
                "sentinel3_scenes": len(s3_scenes),
            }
        )
    except Exception as e:
        print(f"    S3 error: {e}")

    return season


def process_site(site_name: str, site_data: Dict) -> Tuple[str, Dict]:
    """Process all seasons for a single site."""
    # Initialize EE for this thread
    ee.Initialize(project="fluent-optics-344414")

    lat = site_data["lat"]
    lon = site_data["lon"]

    for year, season in site_data["seasons"].items():
        updated_season = process_season(lat, lon, season)
        site_data["seasons"][year].update(
            {
                "sentinel2_scenes": updated_season.get("sentinel2_scenes", 0),
                "sentinel3_scenes": updated_season.get("sentinel3_scenes", 0),
                "s2_cloud_cover_mean": updated_season.get("s2_cloud_cover_mean", 0.0),
                "max_s2_gap_days": updated_season.get("max_s2_gap_days", 0),
                "s2_gap_count": updated_season.get("s2_gap_count", 0),
                "s2_weighted_gap_score": updated_season.get(
                    "s2_weighted_gap_score", 0.0
                ),
            }
        )

    return site_name, site_data


def main():
    """Process satellite scenes for all sites with parallel processing."""
    init_ee()

    # Load GeoJSON data
    with open("all_sites.geojson", "r") as f:
        geojson = json.load(f)

    # Convert GeoJSON to sites dict for processing
    sites = {}
    for feature in geojson["features"]:
        props = feature["properties"]
        sitename = props["sitename"]
        sites[sitename] = {
            "sitename": sitename,
            "lat": feature["geometry"]["coordinates"][1],
            "lon": feature["geometry"]["coordinates"][0],
            "vegetation_type": props["vegetation_type"],
            "description": props["description"],
            "elevation": props["elevation"],
            "country": props.get("country", ""),
            "ndvi_selected": props.get("ndvi_selected", False),
            "seasons": props["seasons"],
        }

    total_sites = len(sites)
    print(f"Processing {total_sites} sites with parallel execution...")
    print("Using 10 parallel threads for efficiency\n")

    # Process sites in parallel
    completed = 0
    with ThreadPoolExecutor(max_workers=10) as executor:
        # Submit all sites for processing
        futures = {
            executor.submit(process_site, site_name, site_data): site_name
            for site_name, site_data in sites.items()
        }

        # Process completed futures
        for future in as_completed(futures):
            site_name = futures[future]
            try:
                processed_name, processed_data = future.result()
                sites[processed_name] = processed_data
                completed += 1
                print(f"[{completed}/{total_sites}] Processed {processed_name}")
            except Exception as e:
                print(f"Error processing {site_name}: {e}")
                completed += 1

    # Update GeoJSON with scene statistics
    for feature in geojson["features"]:
        sitename = feature["properties"]["sitename"]
        if sitename in sites:
            site_data = sites[sitename]
            feature["properties"]["seasons"] = site_data["seasons"]

    # Save updated GeoJSON
    with open("all_sites.geojson", "w") as f:
        json.dump(geojson, f, indent=2)

    print(
        f"\n✅ Updated all_sites.geojson with scene statistics for {total_sites} European sites"
    )


if __name__ == "__main__":
    main()
