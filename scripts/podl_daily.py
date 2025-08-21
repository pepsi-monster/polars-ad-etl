from pathlib import Path
from multi_source_ad_etl.multi_source_ad_etl import MultiSourceAdETL
from google_cloud_client.google_cloud_client import GoogleCloudClient as gcc
import utils.utils as ut
import logging
import polars as pl
import multi_source_ad_etl.data_clean_lib as cln

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

data_dir = Path(__file__).parent.parent / "data"
podl_raw_dir = data_dir / "raw" / "podl"
processed_dir = data_dir / "proc"

# Create folders if they don't exist
data_dir.mkdir(parents=True, exist_ok=True)
podl_raw_dir.mkdir(parents=True, exist_ok=True)
processed_dir.mkdir(parents=True, exist_ok=True)

podl_mapping = {
    "Meta": {
        "Day": "Day",
        "Campaign name": "Campaign name",
        "Ad Set Name": "Ad Set Name",
        "Ad name": "Ad name",
        "Gender": "Gender",
        "Age": "Age",
        "Amount spent (USD)": "Amount spent (USD)",
        "Impressions": "Impressions",
        "Frequency": "Frequency",
        "Reach": "Reach",
        "Unique outbound clicks": "Unique outbound clicks",
        "Link clicks": "Link clicks",
        "Video plays": "Video plays",
        "Video plays at 25%": "Video plays at 25%",
        "Video plays at 50%": "Video plays at 50%",
        "Video plays at 75%": "Video plays at 75%",
        "Video plays at 100%": "Video plays at 100%",
        "Adds to cart": "Adds to cart",
        "Checkouts Initiated": "Checkouts Initiated",
        "Purchases": "Purchases",
        "Purchases conversion value": "Purchases conversion value",
    },
    "TikTok": {
        "By Day": "Day",
        "Campaign name": "Campaign name",
        "Ad group name": "Ad Set Name",
        "Ad name": "Ad name",
        "Cost": "Amount spent (USD)",
        "Impressions": "Impressions",
        "Frequency": "Frequency",
        "Reach": "Reach",
        "Clicks (destination)": "Link clicks",
        "Video views": "Video plays",
        "Video views at 25%": "Video plays at 25%",
        "Video views at 50%": "Video plays at 50%",
        "Video views at 75%": "Video plays at 75%",
        "Video views at 100%": "Video plays at 100%",
        "Adds to cart (website)": "Adds to cart",
        "Checkouts initiated (website)": "Checkouts Initiated",
        "Purchases (website)": "Purchases",
        "Purchase value (website)": "Purchases conversion value",
    },
}

podl_standard_schema = {
    "Source": pl.String,
    "Day": pl.Date,
    "Campaign name": pl.String,
    "Ad Set Name": pl.String,
    "Ad name": pl.String,
    "Gender": pl.String,
    "Age": pl.String,
    "Website URL": pl.String,
    "Amount spent (USD)": pl.Float64,
    "Impressions": pl.Int64,
    "Frequency": pl.Float64,
    "Reach": pl.Int64,
    "Unique outbound clicks": pl.Int64,
    "Link clicks": pl.Int64,
    "Video plays": pl.Int64,
    "Video plays at 25%": pl.Int64,
    "Video plays at 50%": pl.Int64,
    "Video plays at 75%": pl.Int64,
    "Video plays at 100%": pl.Int64,
    "Adds to cart": pl.Int64,
    "Checkouts Initiated": pl.Int64,
    "Purchases": pl.Int64,
    "Purchases conversion value": pl.Float64,
}

podl_source_criteria = {
    "Meta": {"Day", "Gender"},
    "TikTok": {"Cost", "Clicks (destination)"},
}

cleaners = {
    "TikTok": cln.remove_tiktok_total_row,
}

podl = MultiSourceAdETL(
    raw_dir=podl_raw_dir,
    source_criteria=podl_source_criteria,
    rename_mappings=podl_mapping,
    standard_schema=podl_standard_schema,
    cleaning_functions=cleaners,
)

podl_merged = (
    podl.read_tabular_files().assign_source().clean().standardize().merge_and_collect()
)

podl_out = processed_dir / ut.make_date_filename("podl", podl_merged)

daily_exports = {
    "podl": {
        "upload": True,  # True means that upload to the sheet
        "export": True,  # True means that export to the proc dir
        "df": podl_merged,
        "sheet_key": "17-apAkDkg5diJVNeYYCYu7CcCFEn_iPSr3mGk3GWZS4",
        "sheet_name": "raw",  # 👋 Don't forget to change this part!!!!!
        "out": podl_out,
    },
}

# Initializing and setting up Google Cloud service's gspread
gcloud_credential = Path(__file__).parent.parent / "gcloud_credential.json"
gs = gcc(gcloud_credential).googlesheet

for name, config in daily_exports.items():
    export_df: pl.DataFrame = config["df"]
    if config["upload"]:
        # Clear range and notice how the `range_mode = "column_range"`
        gs.clear_range(
            sheet_key=config["sheet_key"],
            sheet_name=config["sheet_name"],
            range=ut.df_to_a1(export_df, range_mode="column_range"),
        )

        # Upload df and notice how the `range_mode = "full_range"`
        gs.upload_dataframe(
            df=export_df,
            sheet_key=config["sheet_key"],
            sheet_name=config["sheet_name"],
            range=ut.df_to_a1(export_df, range_mode="full_range"),
        )

    if config["export"]:
        # Export csv
        export_df.write_csv(config["out"], include_bom=True)
        logging.info(f"File exported to {config['out']}")
