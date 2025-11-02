# PhenoCam Satellite Data Finder

A minimal Python program to find satellite data at PhenoCam network locations.

## Installation

```bash
pip install requests
```

## Usage

```bash
# Find satellite data for first 5 European PhenoCam locations (default)
python main.py

# Find data for 10 locations
python main.py 10

# Run examples
python examples.py
```

## What it does

1. Fetches active PhenoCam camera locations from https://phenocam.nau.edu/api/
2. Queries satellite data at those coordinates using https://earth-search.aws.element84.com/v1/
3. Exports results to `results.json`

## Output

The program creates a JSON file with:
- PhenoCam location details (coordinates, site name, description)
- Number of satellite images found
- Date of latest available image
- Error messages if queries fail

## Python API

```python
# Add src to path for development
import sys
sys.path.insert(0, 'src')

from phenocam_finder import DataFinder

# Basic usage
finder = DataFinder()
locations = finder.get_phenocam_locations()
results = finder.find_data_for_locations(max_locations=5)

# Run examples programmatically
from phenocam_finder import examples
examples.run_all_examples()
```

Default satellite collection is Sentinel-2 L2A with 30-day lookback and <80% cloud cover.
European domain filtering (35°N–71°N, 10°W–40°E) applied automatically.

## Testing

Simple unit tests are included for the main functionality.

```bash
# Install test dependencies
pip install pytest pytest-cov

# Run tests with coverage
PYTHONPATH=src python -m pytest tests/ --cov=src/phenocam_finder --cov-report=term-missing -v
```

Tests cover:
- DataFinder class methods
- Example functions
- Basic error scenarios
