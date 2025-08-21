# Polars-based Multi-Source Advertising Analytics ETL

This project provides a robust ETL (Extract, Transform, Load) pipeline for ingesting, cleaning, and standardizing advertising campaign data from multiple sources like Meta, X (Twitter), and TikTok. It uses the high-performance [Polars](https://pola.rs/) DataFrame library for all data manipulation tasks and integrates with Google Sheets for easy data export and analysis.

The core of this repository is a reusable ETL class that automatically detects the data source, applies source-specific cleaning and transformations, and merges disparate datasets into a single, standardized schema.

## Table of Contents
- [Core Features](#core-features)
- [Project Structure](#project-structure)
- [Setup and Installation](#setup-and-installation)
  - [Prerequisites](#prerequisites)
  - [Installation Steps](#installation-steps)
  - [Google Cloud Authentication](#google-cloud-authentication)
- [How It Works: The ETL Pipeline](#how-it-works-the-etl-pipeline)
- [Usage: Running the ETL Scripts](#usage-running-the-etl-scripts)
- [Core Components](#core-components)
  - [`MultiSourceAdETL` Class](#multisourceadetl-class)
  - [ETL Scripts](#etl-scripts)
  - [Google Cloud Client](#google-cloud-client)
- [Customization](#customization)
- [Development](#development)

## Core Features

- **Multi-Source Ingestion**: Reads raw `.csv` and `.xlsx` files from various advertising platforms.
- **Automatic Source Detection**: Intelligently identifies the source of a file (e.g., Meta, TikTok) based on its columns.
- **Configurable Transformations**: Uses simple dictionary mappings to rename columns and define a final, unified schema.
- **Extensible Data Cleaning**: Apply custom cleaning functions for each data source.
- **High-Performance Processing**: Leverages the Polars library for fast and memory-efficient data manipulation.
- **Automated Export**: Saves processed data to both a local CSV file and a designated Google Sheet.

## Project Structure

```
/
├── .python-version      # Defines the required Python version (3.13)
├── pyproject.toml       # Project metadata and dependencies for `uv`
├── uv.lock              # Pinned versions of dependencies
├── gcloud_credential.json # (Required, not in repo) GCP service account key
├── data/
│   ├── raw/           # Place raw data files for each project here
│   │   ├── apsl/
│   │   ├── mnb/
│   │   └── podl/
│   └── proc/          # Output directory for processed CSV files
├── scripts/             # ETL execution entrypoints
│   ├── apsl_internal.py
│   ├── manaboo_daily.py
│   └── podl_daily.py
├── src/
│   ├── google_cloud_client/
│   │   └── google_cloud_client.py # Wrapper for Google Sheets API
│   ├── multi_source_ad_etl/
│   │   ├── multi_source_ad_etl.py # The core, reusable ETL class
│   │   └── data_clean_lib.py      # Data cleaning helper functions
│   └── utils/
│       └── utils.py               # Utility functions (e.g., filename generation)
└── test/                  # (Empty) Location for future tests
```

## Setup and Installation

### Prerequisites

- **Python 3.13**: The project is locked to this version. We recommend using `pyenv` to manage Python versions.
- **`uv`**: This project uses `uv` for dependency management. It's a fast, modern replacement for `pip` and `venv`. [Installation instructions](https://github.com/astral-sh/uv).
- **Google Cloud Project**: A GCP project with the Google Sheets API and Google Drive API enabled.

### Installation Steps

1.  **Clone the repository:**
    ```bash
    git clone <repository-url>
    cd polars-analytics
    ```

2.  **Set up the Python environment:**
    If you use `pyenv`, the correct version will be picked up automatically. Create and activate the virtual environment using `uv`:
    ```bash
    # Create the virtual environment
    uv venv

    # Activate the environment
    source .venv/bin/activate
    ```

3.  **Install dependencies:**
    ```bash
    uv sync
    ```

### Google Cloud Authentication

This project requires a Google Cloud Service Account to upload data to Google Sheets.

1.  Create a Service Account in your GCP project.
2.  Grant it the "Editor" role (or more restrictive roles that allow editing Google Sheets and Drive files).
3.  Create a JSON key for the service account and download it.
4.  **Rename the key file to `gcloud_credential.json` and place it in the root directory of this project.**

## How It Works: The ETL Pipeline

The ETL process follows these steps:

1.  **Initialization**: An ETL script (e.g., `scripts/apsl_internal.py`) instantiates the `MultiSourceAdETL` class with configuration specific to that pipeline (directory paths, column mappings, schema, etc.).
2.  **Read**: The `read_tabular_files()` method scans the specified `raw` directory for `.csv` and `.xlsx` files and loads them into a list of Polars DataFrames.
3.  **Detect & Assign Source**: The `assign_source()` method iterates through the DataFrames. For each one, it determines the original platform (e.g., "Meta") by checking if the DataFrame's columns contain a unique set of headers defined in the script's `source_criteria` dictionary. It then adds a "Source" column to the DataFrame.
4.  **Clean**: The `clean_dataframes()` method applies any source-specific cleaning functions defined in the script's `cleaners` dictionary. This is useful for tasks like removing total rows from TikTok reports.
5.  **Standardize**: The `standardize_dataframes()` method is the core transformation step. It renames columns based on the `rename_mappings`, adds any missing columns to conform to the `standard_schema`, and casts all columns to their specified data types.
6.  **Merge**: The `merge_and_collect()` method concatenates the processed list of DataFrames into a single, unified DataFrame.
7.  **Export**: The final DataFrame is saved as a date-stamped CSV file in the corresponding `proc` directory and uploaded to the configured Google Sheet.

## Usage: Running the ETL Scripts

To run a specific ETL process, execute its corresponding script from the root directory.

**Example:**

```bash
# Ensure your virtual environment is active
source .venv/bin/activate

# Run the "apsl" daily ETL process
python scripts/apsl_internal.py
```

The script will log its progress to the console and, upon completion, will have:
- Created a new CSV file in `data/proc/`.
- Cleared the target range in the specified Google Sheet and uploaded the new data.

## Core Components

### `MultiSourceAdETL` Class
- **Location**: `src/multi_source_ad_etl/multi_source_ad_etl.py`
- **Purpose**: This is the engine of the project. It's a reusable class that encapsulates the entire ETL workflow. It is initialized with dictionaries that define the behavior for a specific pipeline (which sources to look for, how to rename their columns, etc.).

### ETL Scripts
- **Location**: `scripts/`
- **Purpose**: These are the executable entrypoints for different data pipelines. Each script is responsible for:
    1.  Defining the file paths for its raw and processed data.
    2.  Defining the mappings, schemas, and cleaning functions for its sources.
    3.  Instantiating and running the `MultiSourceAdETL` class.
    4.  Handling the final export to CSV and Google Sheets.

### Google Cloud Client
- **Location**: `src/google_cloud_client/google_cloud_client.py`
- **Purpose**: Provides a simple, high-level interface for interacting with the Google Sheets API. The `GoogleSheetService` class handles authentication, clearing ranges, and uploading Polars DataFrames.

## Customization

To add a new data source to an existing pipeline (e.g., adding "Google Ads" to the `apsl` pipeline):

1.  **Open the script**: Edit `scripts/apsl_internal.py`.
2.  **Add Source Criteria**: Add a new key-value pair to the `apsl_src_criteria` dictionary. The key is the source name ("Google Ads") and the value is a `set` of column names that uniquely identify a raw export from that source.
3.  **Add Rename Mapping**: Add a new key to the `apsl_mapping` dictionary. The value should be another dictionary mapping the raw column names from Google Ads to the standard column names defined in `apsl_standard_schema`.
4.  **Add Cleaning Functions (Optional)**: If the new source requires special cleaning, add a function to `src/multi_source_ad_etl/data_clean_lib.py` and reference it in the `cleaners` dictionary in the script.
5.  **Place Data**: Drop the new raw data file into the `data/raw/apsl/` directory. The script will pick it up on its next run.

## Development

### Testing
Currently, there is no test suite for this project. Adding tests, especially for the `MultiSourceAdETL` class and the cleaning functions, would be a valuable improvement. The `test/` directory is reserved for this purpose.
