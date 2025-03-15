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
   uv pip sync
   ```
   
   This will install dependencies from the `uv.lock` file according to the specifications in `pyproject.toml`.


