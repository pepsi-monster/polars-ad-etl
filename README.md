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
  - [Testing](#testing)

## Core Features

- **Multi-Source Ingestion**: Reads raw `.csv` and `.xlsx` files from various advertising platforms.
- **Automatic Source Detection**: Determines the source of a file (e.g., Meta, TikTok) using user-defined criteria mapped to specific columns.
- **Configurable Transformations**: Uses simple dictionary mappings to rename columns and define a final, unified schema.
- **Extensible Data Cleaning**: Apply custom cleaning functions for each data source.
- **High-Performance Processing**: Leverages the Polars library for fast and memory-efficient data manipulation.
- **Automated Export**: Saves processed data to both a local CSV file and a designated Google Sheet.

## Project Structure

```
┌── .python-version        # Defines the required Python version (3.13)
├── pyproject.toml         # Project metadata and dependencies for `uv`
├── uv.lock                # Pinned versions of dependencies
├── gcloud_credential.json # (Required, not in repo) GCP service account key
├── data/
│   ├── raw/           # Place raw data files for each project here
│   │   ├── apsl/
│   │   ├── mnb/
│   │   └── podl/
│   └── proc/          # Output directory for processed CSV files
├── scripts/           # ETL execution entrypoints
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
└── test/                          # (Empty) Location for future tests
```

## Setup and Installation

### Prerequisites

- **Python 3.13**: The project is locked to this version.
- **`uv`**: This project uses `uv` for dependency management. It's a fast, modern replacement for `pip` and `venv`.
- **Google Cloud Project**: A GCP project with the Google Sheets API and Google Drive API enabled.

### Installation for macOS & Linux

1.  **Install Prerequisites**:

    - **Python 3.13**: We recommend using `pyenv` to manage Python versions (`pyenv install 3.13.0`).
    - **`uv`**: Follow the [official `uv` installation instructions](https://github.com/astral-sh/uv).

2.  **Clone and Set Up Environment**:

    ```bash
    git clone https://github.com/pepsi-monster/polars-ad-etl.git
    cd polars-analytics

    # Create the virtual environment (uv will use the .python-version file)
    uv venv

    # Activate the environment
    source .venv/bin/activate
    ```

3.  **Install Dependencies**:

    ```bash
    # Install dependencies from the lock file
    uv sync

    # Install the project in editable mode
    uv pip install -e .
    ```

### Installation for Windows

1.  **Install Prerequisites**:

    - **Python 3.13**: Download and run the installer from the [official Python website](https://www.python.org/downloads/release/python-3130/). **Important**: Check the box "Add python.exe to PATH" during installation.
    - **`uv`**: Open PowerShell and run:
      ```powershell
      powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
      ```

2.  **Clone and Set Up Environment**:
    Open Command Prompt or PowerShell and run:

    ```powershell
    git clone <repository-url>
    cd polars-analytics

    # Create the virtual environment
    uv venv

    # Activate the environment
    .venv\Scripts\activate
    ```

3.  **Install Dependencies**:

    ```powershell
    # Install dependencies from the lock file
    uv sync

    # Install the project in editable mode
    uv pip install -e .
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
7.  **Export (Handled by Script)**: The calling script (e.g., `scripts/apsl_internal.py`) takes the final merged DataFrame and handles the export. This typically includes saving it as a date-stamped CSV file in the `proc` directory and uploading it to a configured Google Sheet.

## Usage: Running the ETL Scripts

To run a specific ETL process, first activate your virtual environment, then execute the desired script.

**On macOS & Linux:**

```bash
# Ensure your virtual environment is active
source .venv/bin/activate

# Run the "apsl" daily ETL process
python scripts/apsl_internal.py
```

**On Windows:**

```powershell
# Ensure your virtual environment is active
.venv\Scripts\activate

# Run the "apsl" daily ETL process
python scripts\apsl_internal.py
```

The script will log its progress to the console. Upon completion, a successful run will have:

- Created a new CSV file in `data/proc/`.
- Cleared the target range in the specified Google Sheet and uploaded the new data.

### Expected Output

You will see a series of status messages with spinners, indicating the script's progress through the following stages:

- Opening the target Google Sheet.
- Clearing the existing data range.
- Uploading the new DataFrame.
- A final confirmation message when the CSV file has been exported locally.

Example log messages:

```
✅ 'apsl_daily_sheet' opened
✅ Cleared data at 'apsl_daily_sheet' > 'raw' > 'A1:P100'
✅ Uploaded DataFrame to 'apsl_daily_sheet' > 'raw' > 'A1:P101'
2025-08-22 14:30:00 apsl_internal INFO: File exported to data/proc/apsl_2025-08-22.csv
```

_(Note: Spinner animations are not shown in the static log example above.)_

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
- **Purpose**: Provides a simple, high-level interface for interacting with the Google Sheets API. The `GoogleSheetService` class wraps the `gspread` library to offer convenient methods for fetching, clearing, and uploading data using Polars DataFrames. It handles authentication and provides user-friendly feedback during operations.

- **Key Methods**:
  - `get_dataframe()`: Fetches a specified range from a sheet and converts it directly into a Polars DataFrame.
  - `clear_range()`: Clears all values within a specified A1-style range in a worksheet.
  - `upload_dataframe()`: Uploads a Polars DataFrame to a worksheet. It automatically handles the conversion of Polars Date types to Google Sheets' serial number format.

## Customization

To add a new data source to an existing pipeline (e.g., adding "Google Ads" to the `apsl` pipeline):

1.  **Open the script**: Edit `scripts/apsl_internal.py`.
2.  **Add Source Criteria**: Add a new key-value pair to the `apsl_src_criteria` dictionary. The key is the source name ("Google Ads") and the value is a `set` of column names that a raw export from that source.
3.  **Add Rename Mapping**: Add a new key to the `apsl_mapping` dictionary. The value should be another dictionary mapping the raw column names from Google Ads to the standard column names defined in `apsl_standard_schema`.
4.  **Add Cleaning Functions (Optional)**: If the new source requires special cleaning, add a function to `src/multi_source_ad_etl/data_clean_lib.py` and reference it in the `cleaners` dictionary in the script.
5.  **Place Data**: Drop the new raw data file into the appropriate subdirectory within `data/raw/` (e.g., `data/raw/apsl/`). The script will pick it up on its next run.

## Development

### Testing

Currently, there is no test suite for this project. Adding tests, especially for the `MultiSourceAdETL` class and the cleaning functions, would be a valuable improvement. The `test/` directory is reserved for this purpose.
