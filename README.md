# PhenoCam Data Collection

Streamlined Python scripts for collecting and analyzing PhenoCam site data with associated Sentinel-2 and Sentinel-3 satellite imagery statistics via Google Earth Engine.

This toolset fetches phenological camera site information, retrieves satellite scene availability metrics, and calculates NDVI time series for specified locations, providing comprehensive vegetation monitoring data in a simple JSON format.

## Setup

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Authenticate with Google Earth Engine (first time only):
```bash
earthengine authenticate
```

## Usage

### Quick Start

Run all data collection scripts at once:
```bash
python run_all.py
```

### Individual Script Execution

For more control, run each script separately:

#### 1. Fetch PhenoCam Site Data
```bash
python phenocam.py
```
- Reads site IDs from `config.yaml`
- Fetches site information and seasonal data from PhenoCam API
- Creates `all_sites.geojson` with all European PhenoCam sites

#### 2. Get Satellite Scene Statistics
```bash
python get_scenes.py
```
- Reads `all_sites.geojson`
- Queries Sentinel-2 and Sentinel-3 scenes via Google Earth Engine
- Calculates S2 scene count and cloud coverage
- Calculates S2 gap statistics for ALL available scenes (regardless of cloud coverage)
- Calculates S3 scene count only (no other S3 statistics)
- Updates `all_sites.geojson` with scene statistics

#### 3. Calculate NDVI Time Series
```bash
python get_indices.py
```
- Reads `config.yaml` and `all_sites.geojson`
- Calculates NDVI from clear Sentinel-2 imagery (<30% cloud cover)
- Computes mean, min, max, range, and full time series
- Calculates S2 gap statistics for ONLY the clear scenes used in NDVI calculation
- Creates `selected_sites.geojson` with NDVI data and both sets of gap statistics
- Enables comparison: all-scene gaps (satellite availability) vs clear-scene gaps (actual usable data)

### View Results

Open the interactive map visualization to explore the collected data:
```bash
# Open index.html in your browser
python -m http.server 8000
# Then navigate to http://localhost:8000/index.html
```

## Project Structure

### Input Files
- `config.yaml` - Configuration file with site identifiers and NDVI options
- `requirements.txt` - Python dependencies

### Scripts
- `phenocam.py` - Fetches PhenoCam site data and seasonal information
- `get_scenes.py` - Queries satellite scene statistics from Google Earth Engine
- `get_indices.py` - Calculates NDVI time series and statistics
- `run_all.py` - Convenience script to run all data collection scripts

### Output Files
- `all_sites.geojson` - All European PhenoCam sites with scene statistics
- `selected_sites.geojson` - Sites from config.yaml with NDVI time series

### Visualization
- `index.html` - Interactive map visualization

## Gap Statistics

The system calculates two sets of S2 gap statistics to understand data availability:

- **All-scene gaps** (`max_s2_gap_days`, `s2_gap_count`, `s2_weighted_gap_score`): Calculated from all S2 scenes, showing satellite revisit frequency (typically 0-5 days)
- **Clear-scene gaps** (`ndvi_max_s2_gap_days`, `ndvi_s2_gap_count`, `ndvi_s2_weighted_gap_score`): Calculated from scenes with <30% cloud used for NDVI, showing actual monitoring capability (often 10-30+ days)

The difference between these statistics reveals the impact of cloud coverage on vegetation monitoring capabilities at each site.
