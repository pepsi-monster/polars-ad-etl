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
mnb_raw_dir = apsl_dir / "raw" / "mnb"
processed_dir = apsl_dir / "proc"

mnb_meta_mapping = {
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
}

mnb_x_mapping = {
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

mnb = MultiSourceAdETL(mnb_raw_dir)

mnb_merged = (
    mnb.read_tabular_files()
    .assign_source()
    .clean_x_avg_frequency()
    .standardize(
        standard_schema=mnb_standard_schema,
        meta_mapping=mnb_meta_mapping,
        x_mapping=mnb_x_mapping,
    )
    .merge_and_collect()
)

mnb_out_file_name = mnb.construct_file_name("manaboo", mnb_merged)

mnb_out = processed_dir / mnb_out_file_name

mnb_merged.write_csv(mnb_out, include_bom=True)
logging.info(f"File exported to {mnb_out}")
