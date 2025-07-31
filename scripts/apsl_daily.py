from pathlib import Path
from multi_source_ad_etl.multi_source_ad_etl import MultiSourceAdETL
from google_cloud_client.google_cloud_client import GoogleCloudClient as gcc
import utils.utils as ut
import logging
import polars as pl

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

apsl_dir = Path(__file__).parent.parent / "data" / "apsl"
podl_raw_dir = apsl_dir / "raw" / "podl"
processed_dir = apsl_dir / "proc"

podl_meta_mapping = {
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
}

podl_tiktok_mapping = {
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

podl = MultiSourceAdETL(podl_raw_dir)

podl_merged = (
    podl.read_tabular_files()
    .assign_source()
    .clean_tiktok_remove_total()
    .standardize(
        standard_schema=podl_standard_schema,
        meta_mapping=podl_meta_mapping,
        tiktok_mapping=podl_tiktok_mapping,
    )
    .merge_and_collect()
)

podl_out = podl.construct_file_name("podl", podl_merged)

# Initializing and setting up Google Cloud service's gspread
gcloud_credential = Path("/Users/johnny/repos/polars-analytics/gcloud_credential.json")

gs = gcc(gcloud_credential).googlesheet

daily_exports = {
    "podl": {
        "upload": True,
        "df": podl_merged,
        "out": podl_out,
        "sheet_key": "17-apAkDkg5diJVNeYYCYu7CcCFEn_iPSr3mGk3GWZS4",
        "sheet_name": "raw",
        "a1_range": ut.dataframe_to_a1_address(podl_merged),
    },
}

for k, v in daily_exports.items():
    # Exported merged csvs to the computer
    if v["upload"] is True:
        df: pl.DataFrame = v["df"]
        out = processed_dir / v["out"]
        logger.info(f"{k} exported to {out}")
        df.write_csv(out, include_bom=True)
        # Upload df to the sheet
        gs.upload_dataframe(
            df=v["df"],
            sheet_key=v["sheet_key"],
            sheet_name=v["sheet_name"],
            range=v["a1_range"],
        )
