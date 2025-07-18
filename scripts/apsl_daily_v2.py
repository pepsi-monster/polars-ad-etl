from pathlib import Path
from multi_source_ad_etl.multi_source_ad_etl import MultiSourceAdETL
from google_cloud_client.google_cloud_client import GoogleCloudClient as gcc
import utils.utils as ut
import logging
import polars as pl

apsl_dir = Path(__file__).parent.parent / "data" / "apsl"
spr_mrt_dir = apsl_dir / "raw" / "spr_mrt"
breeden_dir = apsl_dir / "raw" / "breeden"

spr_mrt_dfs = [pl.read_csv(f) for f in spr_mrt_dir.glob("*.csv")]
spr_mart_df = pl.concat(spr_mrt_dfs)

breeden_dfs = [pl.read_csv(f) for f in breeden_dir.glob("*.csv")]
breeden_df = pl.concat(breeden_dfs)

# Initializing and setting up Google Cloud service's gspread
gcloud_credential = Path("/Users/johnny/repos/polars-analytics/gcloud_credential.json")

gs = gcc(gcloud_credential).googlesheet

daily_import = {
    "spr_mrt": {
        "upload": True,
        "df": spr_mart_df,
        "sheet_key": "1xTTpduqFNHtZIOhevALzFk94tSFOK8L0m3e2fBZyDRs",
        "sheet_name": "raw",
        "a1_range": ut.dataframe_to_a1_address(spr_mart_df),
    },
    "breeden": {
        "upload": True,
        "df": breeden_df,
        "sheet_key": "1lPjab6dNy-TAtAFo2U5QqevIG4ciBwB61m9zbuigTNc",
        "sheet_name": "raw",
        "a1_range": ut.dataframe_to_a1_address(breeden_df),
    },
}

for k, v in daily_import.items():
    if v["upload"] is True:
        df: pl.DataFrame = v["df"]
        # Upload df to the sheet
        gs.upload_dataframe(
            df=v["df"],
            sheet_key=v["sheet_key"],
            sheet_name=v["sheet_name"],
            range=v["a1_range"],
        )
