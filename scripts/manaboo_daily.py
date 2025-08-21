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
mnb_raw_dir = data_dir / "raw" / "mnb"
processed_dir = data_dir / "proc"

# Create folders if they don't exist
data_dir.mkdir(parents=True, exist_ok=True)
mnb_raw_dir.mkdir(parents=True, exist_ok=True)
processed_dir.mkdir(parents=True, exist_ok=True)

mnb_mapping = {
    "Meta": {
        "Day": "Day",
        "Campaign name": "Campaign name",
        "Ad Set Name": "Ad Set Name",
        "Ad name": "Ad name",
        "Gender": "Gender",
        "Age": "Age",
        "Link (ad settings)": "Link (ad settings)",
        "Amount spent (USD)": "Amount spent (USD)",
        "Impressions": "Impressions",
        "Frequency": "Frequency",
        "Reach": "Reach",
        "Clicks (all)": "Clicks (all)",
        "ThruPlays": "ThruPlays",
        "3-second video plays": "3-second video plays",
        "Registrations Completed": "Registrations Completed",
        "Purchases": "Purchases",
        "Purchases conversion value": "Purchases conversion value",
        "Video plays": "Video plays",
    },
    "X (Twitter)": {
        "Time period": "Day",
        "Campaign name": "Campaign name",
        "Spend": "Amount spent (USD)",
        "Impressions": "Impressions",
        "Average frequency": "Frequency",
        "Total audience reach": "Reach",
        "Clicks": "Clicks (all)",
        "Video completions": "ThruPlays",
        "3s/100% video views": "3-second video plays",
        "Leads": "Registrations Completed",
        "Purchases": "Purchases",
        "Purchases - sale amount": "Purchases conversion value",
        "Video views": "Video plays",
    },
}

mnb_standard_schema = {
    "Source": pl.String,
    "Day": pl.Date,
    "Campaign name": pl.String,
    "Ad Set Name": pl.String,
    "Ad name": pl.String,
    "Gender": pl.String,
    "Age": pl.String,
    "Link (ad settings)": pl.String,
    "Amount spent (USD)": pl.Float64,
    "Impressions": pl.Int64,
    "Frequency": pl.Float64,
    "Reach": pl.Int64,
    "Clicks (all)": pl.Int64,
    "ThruPlays": pl.Int64,
    "3-second video plays": pl.Int64,
    "Registrations Completed": pl.Int64,
    "Purchases": pl.Int64,
    "Purchases conversion value": pl.Float64,
    "Video plays": pl.Int64,
}

mnb_source_criteria = {
    "Meta": {"Campaign name", "Day"},
    "X (Twitter)": {"Objective", "Time period"},
}

cleaners = {"X (Twitter)": cln.clean_x_avg_frequency}

mnb = MultiSourceAdETL(
    raw_dir=mnb_raw_dir,
    source_criteria=mnb_source_criteria,
    rename_mappings=mnb_mapping,
    standard_schema=mnb_standard_schema,
    cleaning_functions=cleaners,
)

mnb_merged = (
    mnb.read_tabular_files()
    .assign_source()
    .clean_dataframes()
    .standardize_dataframes()
    .merge_and_collect()
)

mnb_out = processed_dir / ut.make_date_filename("manaboo", mnb_merged)

daily_exports = {
    "podl": {
        "upload": True,  # True means that upload to the sheet
        "export": True,  # True means that export to the proc dir
        "df": mnb_merged,
        "sheet_key": "1cw5889l9iIKVBRIWdT7B1D6q1eB_cgK1YvK8DbHu5qo",
        "sheet_name": "raw",  # 👋 Don't forget to change this part!!!!!
        "out": mnb_out,
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
