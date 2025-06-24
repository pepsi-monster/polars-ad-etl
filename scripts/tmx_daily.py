# %%
"""
Toomics Analytics Data Processing Pipeline
==========================================

This script processes advertising data from Meta (Facebook) and X (Twitter) platforms,
combines it with internal analytics data, and prepares it for upload to Google Sheets.

Main functions:
1. Read and process Meta CSV files and X Excel files
2. Standardize column names and data types
3. Calculate advertising metrics (CPM, CTR, CPA, ROAS)
4. Convert USD to KRW and apply markup rates
5. Group ad sets into categories
6. Join with internal analytics data
7. Upload processed data to Google Sheets

Author: JungHwan Noh
Last Updated: 2025-06-23
"""

import polars as pl
import polars.selectors as cs
from pathlib import Path
from google_cloud_client.google_cloud_client import GoogleCloudClient as gcc
import utils.utils as ut

# =============================================================================
# CONFIGURATION AND SETUP
# =============================================================================

# Directory containing raw advertising data files
raw_data_dir = Path("/Users/johnny/repos/polars-analytics/data/apsl/raw/toomics_a_g")

# Google Cloud credentials for accessing Google Sheets
gcloud_credential = Path("/Users/johnny/repos/polars-analytics/gcloud_credential.json")
gs = gcc(gcloud_credential).googlesheet

# =============================================================================
# META (FACEBOOK) DATA PROCESSING
# =============================================================================

# %%
# Read Meta CSV files, standardize column names, and add metadata
print("Processing Meta advertising data...")

meta_df = (
    # Read and concatenate all CSV files from the raw data directory
    pl.concat([pl.read_csv(f) for f in raw_data_dir.glob("*.csv")])
    # Rename columns to match standardized naming convention
    # Handle duplicated column names from Meta's export
    .rename(
        {
            "FirstPurchase": "First purchases",
            "FirstPurchase_duplicated_0": "First purchases conversion value",
        }
    )
    # Convert Day column to proper date format
    .cast({"Day": pl.Date})
    # Add source identifier and month grouping
    .with_columns(
        [
            pl.lit("META").alias("Source"),  # Mark as Meta data
            pl.col("Day")
            .dt.strftime("%y-%m")
            .alias("Month"),  # Create month column for grouping
        ]
    )
)

# =============================================================================
# X (TWITTER) DATA PROCESSING
# =============================================================================

# Define column mapping for X data to match Meta column names
x_rename_map = {
    "Time period": "Day",
    "Ad name": "Ad name",
    "Ad Group name": "Ad Set Name",
    "Campaign name": "Campaign name",
    "Average frequency": "Frequency",
    "Website creative URL": "Link (ad settings)",
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

# Process X (Twitter) data with similar transformations as Meta
x_df = (
    # Read and concatenate all Excel files from the raw data directory
    pl.concat([pl.read_excel(f) for f in raw_data_dir.glob("*.xlsx")])
    # Rename columns to match standardized naming convention
    .rename(x_rename_map)
    # Convert Day column to proper date format
    .cast({"Day": pl.Date})
    # Add source identifier and month grouping
    .with_columns(
        [
            pl.lit("X(Twitter)").alias("Source"),  # Mark as X data
            pl.col("Day").dt.strftime("%y-%m").alias("Month"),
        ]
    )
    # Clean frequency data - replace dash with null values
    .with_columns(pl.col("Frequency").replace_strict("-", None))
)

# =============================================================================
# DATA COMBINATION AND CLEANING
# =============================================================================

# Find common columns between Meta and X data for consistent combination
common_columns = set(x_df.columns) & set(meta_df.columns)

# Combine Meta and X data, keeping only common columns
combined_df = pl.concat([meta_df, x_df], how="diagonal_relaxed").select(common_columns)

# Create filter for numeric columns that are finite and non-zero
# This removes invalid data points and zero-spend campaigns
numeric_nonzero_filter = cs.numeric().is_finite() & (cs.numeric() != 0)

# Apply filter to remove rows with invalid or zero numeric values
combined_df = combined_df.filter(pl.any_horizontal(numeric_nonzero_filter))

# =============================================================================
# DATA AGGREGATION
# =============================================================================

# %%
# Aggregate data by key dimensions to avoid double-counting
print("Aggregating advertising data...")

aggregated_df = (
    combined_df.group_by(
        ["Source", "Day", "Campaign name", "Ad Set Name", "Ad name", "Month"]
    )
    # Sum all numeric columns for each group
    .agg(cs.numeric().sum())
    # Remove columns that shouldn't be summed (frequency is average, not sum)
    .select(pl.exclude(["Clicks (all)", "Frequency"]))
)

# =============================================================================
# CURRENCY CONVERSION AND COST CALCULATIONS
# =============================================================================

# Define columns that need currency conversion from USD to KRW
usd_columns = [
    "Amount spent (USD)",
    "First purchases conversion value",
    "Purchases conversion value",
]

# Exchange rate and markup configuration
usd_to_krw_rate = 1330  # Current USD to KRW exchange rate
apsl_markup_rate = 1 + 0.136  # 13.6% agency markup rate

# Convert USD columns to KRW
aggregated_df = aggregated_df.with_columns(
    [
        (cs.by_name(col) * usd_to_krw_rate).alias(col.replace("USD", "KRW"))
        for col in usd_columns
    ]
).with_columns(
    # Apply agency markup to advertising spend
    (cs.by_name("Amount spent (KRW)") * apsl_markup_rate).alias(
        "광고비 (적용 요율: 13.6%)"
    )
)

# =============================================================================
# MARKETING METRICS CALCULATION
# =============================================================================

# Define formulas for key advertising metrics
metric_expressions = {
    "CPM": (
        (pl.col("광고비 (적용 요율: 13.6%)") / pl.col("Impressions")) * 1000
    ),  # Cost per mille
    "CTR": (pl.col("Link clicks") / pl.col("Impressions")),  # Click-through rate
    "CPA (가입)": (
        pl.col("광고비 (적용 요율: 13.6%)") / pl.col("Registrations Completed")
    ),  # Cost per acquisition (registration)
    "CPA (결제)": (
        pl.col("광고비 (적용 요율: 13.6%)") / pl.col("Purchases")
    ),  # Cost per acquisition (purchase)
    "CPA (첫 결제)": (
        pl.col("광고비 (적용 요율: 13.6%)") / pl.col("First purchases")
    ),  # Cost per first purchase
    "ROAS (결제)": (
        pl.col("Purchases conversion value") / pl.col("광고비 (적용 요율: 13.6%)")
    ),  # Return on ad spend (all purchases)
    "ROAS (첫 결제)": (
        pl.col("First purchases conversion value") / pl.col("광고비 (적용 요율: 13.6%)")
    ),  # Return on ad spend (first purchases)
}

metric_names = metric_expressions.keys()

# Calculate metrics and handle infinite/null values
aggregated_df = aggregated_df.with_columns(
    [expr.alias(col) for col, expr in metric_expressions.items()]
).with_columns(
    # Replace infinite values with 0 for cleaner reporting
    [
        pl.when(pl.col(col).is_finite()).then(pl.col(col)).otherwise(0)
        for col in metric_expressions
    ]
)

# =============================================================================
# AD SET GROUPING AND CATEGORIZATION
# =============================================================================

# Get unique ad set names for validation
unique_adsets = sorted((aggregated_df["Ad Set Name"].unique().to_list()))

# Define mapping from specific ad set names to broader categories
# This helps in reporting and analysis by grouping similar ad sets
adset_group_map = {
    # Test campaigns
    "1_Pending premium verification_troubleshoot": "X_광고_테스트용",
    "2_Pending premium verification_troubleshoot": "X_광고_테스트용",
    # Lookalike audiences (유사타겟)
    "PUR_1ST_M_LAL10%_Pur 180D&Entire_241209": "앱실론_유사타겟",
    "PUR_1ST_M_LAL10%_Pur 360D_250311": "앱실론_유사타겟",
    "PUR_CAPI_M_LAL5%_Reg 180D_240801": "앱실론_유사타겟",
    "PUR_CAPI_M_LAL5%_Reg 180D_250311": "앱실론_유사타겟",
    # Retargeting campaigns (리타겟팅)
    "PUR_CAPI_A_DIR_Visitors 7D_240819": "앱실론_리타겟팅",
    "PUR_HYBRID_A_DIR_Pur 180D&365D_240816": "앱실론_리타겟팅",
    "PUR_HYBRID_A_DIR_Pur 180D&360D_250311": "앱실론_리타겟팅",
    "PUR_HYBRID_A_DIR_Pur 180D&360D_250623": "앱실론_리타겟팅",
    # Value optimization campaigns (가치최적화)
    "PUR_META_AUTO_ASC_Mature Adult_241226": "앱실론_가치최적화",
    "PUR_META_AUTO_ASC_Young Adult_240801": "앱실론_가치최적화",
    # First purchase campaigns (첫결제)
    "PUR_META_M_ASC_High firstPurchase only Age 18-24_250317": "앱실론_첫결제",
    "PUR_META_M_ASC_High firstPurchase only Age 25-34_250317": "앱실론_첫결제",
    # Interest-based campaigns (관심사)
    "PUR_META_M_INT_Cosplay Age 20-48_240319": "앱실론_관심사",
    "PUR_META_M_INT_Gaming Console Age 21-48_240805": "앱실론_관심사",
    "PUR_META_M_INT_GirlsComic Age 21-48_240801": "앱실론_관심사",
    "PUR_META_M_INT_GirlsComic Age 21-48_250428": "앱실론_관심사",
    "PUR_META_M_INT_Manga&Cosplay Age 21-48_250519": "앱실론_관심사",
    "PUR_META_M_INT_Politics Age 35-65+_250305": "앱실론_관심사",
    "PUR_META_M_INT_Politics Age 35-65+_250616": "앱실론_관심사",
    # Broad targeting campaigns (브로드)
    "PUR_META_M_NOT_Broad Detarget existing users_240801": "앱실론_브로드",
    "PUR_META_M_NOT_Broad_250307": "앱실론_브로드",
    "PUR_META_M_NOT_Broad_250528": "앱실론_브로드",
    # X (Twitter) campaigns
    "PUR_X_M_FL-LAL_Politicians 21-65+_250411": "트위터_유사타겟01",
    "PUR_X_M_FL-LAL_Webtoon artists 21-65+_250411": "트위터_유사타겟02",
    "PUR_X_M_INT_Automotive 21-65+_250411": "트위터-관심사01",
    "PUR_X_M_INT_Gaming 21-65+_250408": "트위터_관심사",
    "PUR_X_M_KWD_Adult webtoon 21-65+_250411": "트위터_키워드01",
    "PUR_X_M_NOT_Broad_250318": "트위터_브로드",
    "REG_X_M NOT_Broad_250429": "트위터_회원가입",
}

# Validate that all ad sets have been mapped
missing_adset_keys = set(unique_adsets) - set(adset_group_map)

if missing_adset_keys:
    missing = "\n".join(sorted(missing_adset_keys))
    raise ValueError(f"Missing mapping for:\n{missing}")

# Apply ad set grouping
aggregated_df = aggregated_df.with_columns(
    pl.col("Ad Set Name").replace_strict(adset_group_map).alias("그룹")
)

# =============================================================================
# FINAL DATA PREPARATION FOR REPORTING
# =============================================================================

# Define column renaming for Korean reporting interface
column_rename_map = {
    "Day": "날짜",
    "그룹": "그룹",
    "광고비 (적용 요율: 13.6%)": "광고비 (적용 요율: 13.6%)",
    "Source": "구분1",
    "Campaign name": "캠페인 명",
    "Ad name": "광고 명",
    "Impressions": "노출",
    "Link clicks": "링크 클릭",
    "Registrations Completed": "가입 수 (매체)",
    "Purchases": "결제 수 (매체)",
    "Purchases conversion value": "총 결제 금액 (매체)",
    "First purchases": "첫결제 수 (매체)",
    "First purchases conversion value": "총 첫결제 금액 (매체)",
    "CPM": "CPM",
    "CTR": "CTR",
    "CPA (가입)": "CPA (가입)",
    "CPA (결제)": "CPA (결제)",
    "CPA (첫 결제)": "CPA (첫 결제)",
    "ROAS (결제)": "ROAS (결제)",
    "ROAS (첫 결제)": "ROAS (첫 결제)",
    "Month": "월",
}

# Create final aggregated dataframe with Korean column names
final_aggregated_df = aggregated_df.select(pl.col(column_rename_map.keys())).rename(
    column_rename_map
)

# =============================================================================
# COMBINED RAW DATA PREPARATION
# =============================================================================

# Create a combined raw data export with essential columns
final_combined_df = (
    combined_df.select(
        [
            "Source",
            "Day",
            "Campaign name",
            "Ad Set Name",
            "Ad name",
            "Link (ad settings)",
            "Amount spent (USD)",
            "Impressions",
            "Frequency",
            "Clicks (all)",
            "Link clicks",
            "Registrations Completed",
            "Purchases",
            "Purchases conversion value",
            "First purchases",
            "First purchases conversion value",
        ]
    )
    # Fill missing values with 0 for cleaner data
    .fill_nan(0)
    .fill_null(0)
).with_columns(
    # Standardize source names for reporting
    pl.col("Source").replace_strict(
        {
            "META": "Meta",
            "X(Twitter)": "X (Twitter)",
        }
    )
)

# =============================================================================
# INTERNAL DATA INTEGRATION
# =============================================================================

# Define schema for internal analytics data from Google Sheets
internal_schema_map = {
    "날짜": pl.Date,
    "구분1": pl.String,
    "그룹": pl.String,
    "뷰수(ad)": pl.Int64,
    "로그인(ad)": pl.Int64,
    "가입수(ad)": pl.Int64,
    "인증수(ad)": pl.Int64,
    "첫결제수(ad)": pl.Int64,
    "첫결제금액(ad)": pl.Float64,
    "총결제수(ad)": pl.Int64,
    "총결제금액(ad)": pl.Float64,
}

# Read internal analytics data from Google Sheets
print("Reading internal analytics data from Google Sheets...")
internal_df = (
    gs.get_dataframe(
        "1JCl7-oZWUeGJwF87ggARxlxK8StAaKCFlu7CTfe1MXs", "투믹스(raw)", "A2:K"
    )
    .cast({"날짜": pl.Date})
    .with_columns(
        # Remove commas from numeric string columns
        pl.col(
            [
                col
                for col in internal_schema_map.keys()
                if col not in ["날짜", "구분1", "그룹"]
            ]
        ).str.replace_all(",", "")
    )
    .cast(internal_schema_map)
)

# Define join keys for combining with internal analytics data
join_keys = ["날짜", "구분1", "그룹"]

# Aggregate advertising data for joining with internal data
aggregated_for_join_df = final_aggregated_df.group_by(join_keys).agg(
    (cs.numeric().exclude(metric_names)).sum()
)

# Join advertising data with internal analytics data
final_joined_df = aggregated_for_join_df.join(
    internal_df,
    on=join_keys,
    how="left",
)


# Validate that totals match
combined_total = final_combined_df["Impressions"].sum()
aggregated_total = final_aggregated_df["노출"].sum()
aggregated_joined_total = aggregated_for_join_df["노출"].sum()

# =============================================================================
# DATA UPLOAD CONFIGURATION
# =============================================================================

# Control flags for uploading different datasets to Google Sheets
upload_flags = {
    "final_combined_df": True,  # Raw combined advertising data
    "final_aggregated_df": True,  # Aggregated advertising data with metrics
    "final_joined_df": True,  # Joined advertising + internal data
}

# =============================================================================
# GOOGLE SHEETS UPLOAD (CONDITIONAL)
# =============================================================================

# Upload raw combined data if flag is enabled
if upload_flags.get("final_combined_df"):
    print("Uploading combined raw data to Google Sheets...")
    gs.upload_dataframe(
        df=final_combined_df,
        sheet_key="1nRsC_vBdkJfn0WnM9Iu4NOJECn1G5UbSv0NRIbsgD4w",
        sheet_name="raw",
        range=ut.dataframe_to_a1_address(final_combined_df),
    )

# Upload aggregated data if flag is enabled
if upload_flags.get("final_aggregated_df"):
    print("Uploading aggregated data to Google Sheets...")
    gs.upload_dataframe(
        df=final_aggregated_df,
        sheet_key="1JCl7-oZWUeGJwF87ggARxlxK8StAaKCFlu7CTfe1MXs",
        sheet_name="raw",
        range=ut.dataframe_to_a1_address(final_aggregated_df, vertical_offset=1),
    )

# Upload joined data if flag is enabled
if upload_flags.get("final_joined_df"):
    print("Uploading joined data to Google Sheets...")
    gs.upload_dataframe(
        df=final_joined_df,
        sheet_key="1JCl7-oZWUeGJwF87ggARxlxK8StAaKCFlu7CTfe1MXs",
        sheet_name="raw_joined",
        range=ut.dataframe_to_a1_address(final_joined_df),
    )

print("Data processing pipeline completed successfully!")
