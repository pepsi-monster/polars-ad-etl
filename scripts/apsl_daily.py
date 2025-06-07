from pathlib import Path
from plETL.apslETL import apslETL
import logging
import polars as pl

apsl_dir = Path(__file__).parent.parent / "data" / "apsl"
toomics_raw_dir = apsl_dir / "raw" / "toomics"
processed_dir = apsl_dir / "proc"

toomics = apslETL(toomics_raw_dir)

toomics_meta_mapping = {
    "FirstPurchase": "First purchases",
    "FirstPurchase_duplicated_0": "First purchases conversion value",
}

toomics_x_mapping = {
    "Time period": "Day",
    "Ad Group name": "Ad Set Name",
    "Campaign name": "Campaign name",
    "Average frequency": "Frequency",
    "Spend": "Amount spent (USD)",
    "Impressions": "Impressions",
    "Clicks": "Clicks (all)",
    "Link clicks": "Link clicks",
    "Leads": "Registrations Completed",
    "Purchases": "Purchases",
    "Purchases - sale amount": "Purchases conversion value",
    "Custom events": "First purchases",
    "Custom events - sale amount": "First purchases conversion value",
}

toomics_schema = {
    "Source": pl.String,
    "Day": pl.Date,
    "Campaign name": pl.String,
    "Ad Set Name": pl.String,
    "Ad name": pl.String,
    "Age": pl.String,
    "Gender": pl.String,
    "Amount spent (USD)": pl.Float64,
    "Impressions": pl.Int64,
    "Clicks (all)": pl.Int64,
    "Link clicks": pl.Int64,
    "Frequency": pl.Float64,
    "Registrations Completed": pl.Int64,
    "Purchases": pl.Int64,
    "Purchases conversion value": pl.Float64,
    "First purchases": pl.Int64,
    "First purchases conversion value": pl.Float64,
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

daily_exports = {}

toomics_merged = (
    toomics.read_tabular_files()
    .assign_source()
    .clean_x_avg_frequency()
    .standardize(
        standard_schema=toomics_schema,
        meta_mapping=toomics_meta_mapping,
        x_mapping=toomics_x_mapping,
    )
    .merge_and_collect()
)

# Construct output filename based on data date range: 'client_name'_{min_date}–{max_date}.csv
toomics_out = (
    processed_dir / f"{toomics.construct_file_name('toomics', toomics_merged)}"
)

logging.info(f"Exported to {toomics_out}")
toomics_merged.write_csv(toomics_out)
