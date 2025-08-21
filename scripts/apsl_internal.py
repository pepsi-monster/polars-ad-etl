from pathlib import Path
from multi_source_ad_etl.multi_source_ad_etl import MultiSourceAdETL
from google_cloud_client.google_cloud_client import GoogleCloudClient as gcc
import multi_source_ad_etl.data_clean_lib as cln
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
apsl_raw_dir = apsl_dir / "raw" / "apsl"
processed_dir = apsl_dir / "proc"

apsl_mapping = {
    "Meta": {
        "Day": "Day",
        "Account name": "Account name",
        "Campaign name": "Campaign name",
        "Ad Set Name": "Ad Set Name",
        "Ad name": "Ad name",
        "Amount spent (USD)": "Amount spent (USD)",
        "Impressions": "Impressions",
        "Reach": "Reach",
        "Frequency": "Frequency",
        "Link clicks": "Link clicks",
        "Registrations Completed": "Registrations Completed",
        "Adds to cart": "Adds to cart",
        "Checkouts Initiated": "Checkouts Initiated",
        "Purchases": "Purchases",
        "Purchases conversion value": "Purchases conversion value",
    },
    "X (Twitter)": {
        "Time period": "Day",
        "Funding source name": "Account name",
        "Campaign name": "Campaign name",
        "Ad Group name": "Ad Set Name",
        "Spend": "Amount spent (USD)",
        "Impressions": "Impressions",
        "Link clicks": "Link clicks",
        "Leads": "Registrations Completed",
        "Cart additions": "Adds to cart",
        "Checkouts initiated": "Checkouts Initiated",
        "Purchases": "Purchases",
        "Purchases - sale amount": "Purchases conversion value",
    },
    "TikTok": {
        "By Day": "Day",
        "Account name": "Account name",
        "Campaign name": "Campaign name",
        "Ad group name": "Ad Set Name",
        "Ad name": "Ad name",
        "Cost": "Amount spent (USD)",
        "Impressions": "Impressions",
        "Frequency": "Frequency",
        "Reach": "Reach",
        "Clicks (destination)": "Link clicks",
        "Adds to cart (website)": "Adds to cart",
        "Checkouts initiated (website)": "Checkouts Initiated",
        "Purchases (website)": "Purchases",
        "Purchase value (website)": "Purchases conversion value",
    },
}

apsl_standard_schema = {
    "Day": pl.Date,
    "Source": pl.String,
    "Account name": pl.String,
    "Campaign name": pl.String,
    "Ad Set Name": pl.String,
    "Ad name": pl.String,
    "Amount spent (USD)": pl.Float64,
    "Impressions": pl.Int64,
    "Reach": pl.Int64,
    "Frequency": pl.Float64,
    "Link clicks": pl.Int64,
    "Registrations Completed": pl.Int64,
    "Adds to cart": pl.Int64,
    "Checkouts Initiated": pl.Int64,
    "Purchases": pl.Int64,
    "Purchases conversion value": pl.Float64,
}

apsl_src_criteria = {
    "Meta": {"Day", "Purchases conversion value"},
    "X (Twitter)": {"Time period", "Cart additions"},
    "TikTok": {"Cost", "Clicks (destination)"},
}

cleaners = {
    "TikTok": cln.remove_tiktok_total_row,
    "X (Twitter)": cln.clean_x_avg_frequency,
}

apsl = MultiSourceAdETL(
    raw_dir=apsl_raw_dir,
    source_criteria=apsl_src_criteria,
    rename_mappings=apsl_mapping,
    standard_schema=apsl_standard_schema,
    cleaning_functions=cleaners,
)

apsl_merged = (
    apsl.read_tabular_files()
    .assign_source()
    .clean_dataframes()
    .standardize_dataframes()
    .merge_and_collect()
)

apsl_out = processed_dir / ut.make_date_filename("apsl", apsl_merged)

daily_exports = {
    "apsl": {
        "upload": True,  # True means that upload to the sheet
        "export": True,  # True means that export to the proc dir
        "df": apsl_merged,
        "sheet_key": "1zX87QulsAnrHR03zpVCLc2Ophcn-oVx1kimtPsfJgTE",
        "sheet_name": "raw",  # 👋 Don't forget to change this part!!!!!
        "out": apsl_out,
    },
}

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
