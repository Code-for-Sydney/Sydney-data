# GIS Analysis of Sydney Open Data
## Part 1: Real estate transaction

This repository contains some ETL scripts for Code for Sydney Hackathon
GIS project on Analysing real estate market.

You need to create a Kaggle account to download the dataset from [here](https://www.kaggle.com/datasets/josephcheng123456/nsw-australia-property-data).

## Installation

### Python 3.13
1. Download Python 3.13 from the [official Python website](https://www.python.org/downloads/)
2. Follow the installation instructions for your operating system
3. Verify installation by running `python --version` in your terminal

### UV Package Manager
1. Install UV using the official installation script:
   ```bash
   curl -fsSL https://raw.githubusercontent.com/astral-sh/uv/main/install.sh | bash
   ```
   
   Or on Windows:
   ```bash
   powershell -c "irm https://raw.githubusercontent.com/astral-sh/uv/main/install.ps1 | iex"
   ```

2. Verify installation:
   ```bash
   uv --version
   ```

3. Create a virtual environment and install dependencies:
   ```bash
   uv venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   uv sync
   ```
   
   This will install dependencies from the `uv.lock` file according to the specifications in `pyproject.toml`.

### Redis Installation
1. Install Redis using your package manager:
   ```bash
   # macOS (using Homebrew)
   brew install redis

   # Ubuntu/Debian
   sudo apt-get install redis-server

   # Windows (using WSL2 recommended)
   sudo apt-get install redis-server
   ```

2. Start Redis server:
   ```bash
   # macOS/Linux
   redis-server

   # As a service on Ubuntu/Debian
   sudo systemctl start redis-server
   ```

3. Verify Redis is running:
   ```bash
   redis-cli ping
   ```
   Should return "PONG"

### Nominatim Setup
1. Install Docker if not already installed
2. Pull and run the Nominatim Docker image with Australia's map data:
   ```bash
   docker run -it \
     -e PBF_URL=https://download.geofabrik.de/australia-oceania/australia-latest.osm.pbf \
     -e REPLICATION_URL=https://download.geofabrik.de/australia-oceania/australia-updates/ \
     -p 8080:8080 \
     --name nominatim \
     mediagis/nominatim:4.5
   ```

## Using the Scripts

### 1. Filtering the Dataset

First, filter the raw Kaggle dataset to extract only properties in Sydney suburbs:

```bash
python filter.py
```

The script:
- Reads the raw Kaggle dataset from `nsw_property_data.csv`
- Filters properties based on a predefined list of Sydney suburbs
- Saves the filtered data to `sydney_property_data.csv` with tab separation

If you need to modify the script to include different suburbs or filtering criteria, you can edit the `sydney_suburbs` list in `filter.py`.

### 2. Geocoding the Filtered Data

The geocoding script uses Redis for caching to improve performance and reduce load on the Nominatim server:

1. Ensure Redis and Nominatim are running
2. Run the geocoding script:
   ```bash
   python batch_geocode_local.py
   ```

This will:
- Read the filtered data from `sydney_property_data.csv`
- Check Redis cache for previously geocoded addresses
- Geocode new addresses using the local Nominatim server
- Cache successful geocoding results in Redis (30-day expiry)
- Save the results to `sydney_property_data_geocoded_no_unit.csv`

Features:
- Multi-threaded processing for faster geocoding
- Automatic unit number stripping for better matches
- Graceful fallback if Redis is unavailable
- Progress logging and performance metrics

## Testing

The project includes a comprehensive test suite that verifies:
- Redis caching functionality
- Nominatim geocoding
- Address processing
- Error handling and fallbacks

Run the tests with:
```bash
python -m pytest test_batch_geocode_local.py -v
```

The tests require:
- A running Redis instance on localhost:6379
- A running Nominatim server on localhost:8080

The test suite will automatically skip tests if either service is unavailable.




