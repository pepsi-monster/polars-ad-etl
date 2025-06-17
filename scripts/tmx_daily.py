from pathlib import Path
from google_cloud_client.google_cloud_client import GoogleCloudClient as gcc
import utils.utils as ut
import polars as pl
from multi_source_ad_etl.multi_source_ad_etl import MultiSourceAdETL
import logging


# logging...
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ======================================================================================
# Step 1. Multi source .csv and .xlsx -> Unified dataframe
# ======================================================================================
toomics_raw_dir = Path(__file__).parent.parent / "data" / "apsl" / "raw" / "toomics_new"
proc_dir = Path(__file__).parent.parent / "data" / "apsl" / "proc"

toomics = MultiSourceAdETL(toomics_raw_dir)

toomics_meta_mapping = {
    "FirstPurchase": "First purchases",
    "FirstPurchase_duplicated_0": "First purchases conversion value",
}

toomics_x_mapping = {
    "Time period": "Day",
    "Ad Group name": "Ad Set Name",
    "Campaign name": "Campaign name",
    "Website creative URL": "Link (ad settings)",
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
    "Link (ad settings)": pl.String,
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

# Using unified merging operation to get the merged raw
df = (
    toomics.read_tabular_files()
    .assign_source()
    .clean_x_avg_frequency()
    .standardize(
        standard_schema=toomics_schema,
        meta_mapping=toomics_meta_mapping,
        x_mapping=toomics_x_mapping,
    )
    .merge_and_collect()
).filter(pl.col("Amount spent (USD)") != 0)

# ======================================================================================
# Step 2. Unified dataframe → Toomics internal compatible raw sheet
# ======================================================================================

# To map Meta and Twitter ad set names to Toomics's internal grouping
toomics_ad_set_mapping = {  # MUST ADD MORE WHEN NEW AD SETS ARE ADDED
    "1_Pending premium verification_troubleshoot": "",
    "2_Pending premium verification_troubleshoot": "",
    "DO NOT USE_PUR_META_AUTO_ASC_Mature Adult_241226": "앱실론_가치최적화",
    "PUR_1ST_M_LAL10%_Pur 180D&Entire_241209": "앱실론_유사타겟",
    "PUR_1ST_M_LAL10%_Pur 360D_250311": "앱실론_유사타겟",
    "PUR_CAPI_A_DIR_Visitors 7D_240819": "앱실론_리타겟팅",
    "PUR_CAPI_M_LAL5%_Reg 180D_240801": "앱실론_유사타겟",
    "PUR_CAPI_M_LAL5%_Reg 180D_250311": "앱실론_유사타겟",
    "PUR_HYBRID_A_DIR_Pur 180D&360D_250311": "앱실론_리타겟팅",
    "PUR_HYBRID_A_DIR_Pur 180D&365D_240816": "앱실론_리타겟팅",
    "PUR_META_AUTO_ASC_ASC Campaign-Video_241119": "앱실론_가치최적화",
    "PUR_META_AUTO_ASC_Mature Adult_241226": "앱실론_가치최적화",
    "PUR_META_AUTO_ASC_Young Adult_240801": "앱실론_가치최적화",
    "PUR_META_M_ASC_High firstPurchase only Age 18-24_250317": "앱실론_첫결제",
    "PUR_META_M_ASC_High firstPurchase only Age 25-34_250317": "앱실론_첫결제",
    "PUR_META_M_INT_Cosplay Age 20-48_240319": "앱실론_관심사",
    "PUR_META_M_INT_Gaming Console Age 21-48_240805": "앱실론_관심사",
    "PUR_META_M_INT_GirlsComic Age 21-48_240801": "앱실론_관심사",
    "PUR_META_M_INT_GirlsComic Age 21-48_250428": "앱실론_관심사",
    "PUR_META_M_INT_Manga&Cosplay Age 21-48_250519": "앱실론_관심사",
    "PUR_META_M_INT_Politics Age 35-65+_250305": "앱실론_관심사",
    "PUR_META_M_INT_Politics Age 35-65+_250616": "앱실론_관심사",
    "PUR_META_M_NOT_Broad Detarget existing users_240801": "앱실론_브로드",
    "PUR_META_M_NOT_Broad Detarget existing users_250307": "앱실론_브로드",
    "PUR_META_M_NOT_Broad_250307": "앱실론_브로드",
    "PUR_META_M_NOT_Broad_250528": "앱실론_브로드",
    "PUR_X_M NOT_Broad_250502": "트위터_브로드",
    "PUR_X_M_FL-LAL_Politicians 21-65+_250411": "트위터_유사타겟01",
    "PUR_X_M_FL-LAL_Webtoon artists 21-65+_250411": "트위터_유사타겟02",
    "PUR_X_M_INT_Automotive 21-65+_250411": "트위터-관심사01",
    "PUR_X_M_INT_Gaming 21-65+_250408": "트위터_관심사",
    "PUR_X_M_KWD_Adult webtoon 21-65+_250411": "트위터_키워드01",
    "PUR_X_M_NOT_Broad_250318": "트위터_브로드",
    "REG_X_M NOT_Broad_250429": "트위터_회원가입",
}

# `df` has 'Gender' and 'Age', so its lower granularity.
# Going up the granularity by grouping and generating a '그룹' column, and renaming sources to match Toomics internal data.
toomics_merged = (
    df.with_columns(pl.col("Ad Set Name").replace(toomics_ad_set_mapping).alias("그룹"))
    .group_by(["Day", "그룹", "Campaign name", "Ad name", "Source"])
    .agg(pl.all().exclude(["Day", "그룹", "Campaign name", "Ad name", "Source"]).sum())
).with_columns(pl.col("Source").replace({"Meta": "META", "X (Twitter)": "X(Twitter)"}))

# Now convert USD to KRW, and the ad spend is also multiplied by 1.136 to accommodate the agency rate per the client's request.
toomics_merged = toomics_merged.with_columns(
    [
        (pl.col("Amount spent (USD)") * 1330 * 1.136),
        (pl.col("Purchases conversion value") * 1330),
        (pl.col("First purchases conversion value") * 1330),
    ]
)
unified_col_names_mapping = {
    "Day": "날짜",
    "그룹": "그룹",
    "Amount spent (USD)": "광고비 (KRW)",
    "Source": "구분1",
    "Campaign name": "캠페인 명",
    "Ad name": "광고 명",
    "Impressions": "노출",
    "Link clicks": "링크 클릭",
    "Registrations Completed": "매체 총 가입 수",
    "Purchases": "매체 총 결제 수",
    "Purchases conversion value": "매체 총 결제 금액",
    "First purchases": "매체 총 첫 결제 수",
    "First purchases conversion value": "매체 총 첫 결제 금액",
    "CPM": "CPM",
    "CTR (링크 클릭)": "CTR (링크 클릭)",
    "CVR (가입)": "CVR (가입)",
    "CVR (결제)": "CVR (결제)",
    "CVR (첫 결제)": "CVR (첫 결제)",
    "ROAS (결제)": "ROAS (결제)",
    "ROAS (첫 결제)": "ROAS (첫 결제)",
}

# Adding calculated metrics
toomics_merged = (
    toomics_merged.with_columns(
        [
            ((pl.col("Amount spent (USD)")) / (pl.col("Impressions")) * 1000).alias(
                "CPM"
            ),
            ((pl.col("Link clicks")) / (pl.col("Impressions"))).alias(
                "CTR (링크 클릭)"
            ),
            ((pl.col("Registrations Completed")) / (pl.col("Link clicks"))).alias(
                "CVR (가입)"
            ),
            ((pl.col("Purchases")) / (pl.col("Link clicks"))).alias("CVR (결제)"),
            ((pl.col("First purchases")) / (pl.col("Link clicks"))).alias(
                "CVR (첫 결제)"
            ),
            (
                (pl.col("Purchases conversion value")) / (pl.col("Amount spent (USD)"))
            ).alias("ROAS (결제)"),
            (
                (pl.col("First purchases conversion value"))
                / (pl.col("Amount spent (USD)"))
            ).alias("ROAS (첫 결제)"),
        ]
    )
    # Selecting only the needed columns and renaming them to better align with the internal schema
    .select(unified_col_names_mapping.keys())
    .rename(unified_col_names_mapping)
).sort(pl.col("날짜"), descending=True)


float_cols = [
    col
    for col, dtype in toomics_merged.schema.items()
    if dtype in [pl.Float64, pl.Float32]
]

# Handling NaN and inf... for smooth uploading to the google sheet

toomics_merged = toomics_merged.with_columns(
    [
        pl.col(col).replace([float("inf"), float("-inf")], None).name.keep()
        for col in float_cols
    ]
).fill_nan(None)

# ======================================================================================
# Step 3. Toomics internal compatible raw sheet -> tmx_data_for_join
# ======================================================================================

# this df is going to be used for join operation
tmx_data_for_join = toomics_merged.group_by(["날짜", "구분1", "그룹"]).agg(
    pl.col(
        [
            "광고비 (KRW)",
            "노출",
            "링크 클릭",
            "매체 총 가입 수",
            "매체 총 결제 수",
            "매체 총 결제 금액",
            "매체 총 첫 결제 수",
            "매체 총 첫 결제 금액",
        ]
    ).sum()
)

internal_schema = {
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

# ======================================================================================
# Step 4. Join it now!
# ======================================================================================

gcloud_credential = Path("/Users/johnny/repos/polars-analytics/gcloud_credential.json")
gs = gcc(gcloud_credential).googlesheet

# because we need to join tmx_
tmx_internal_data = (
    gs.get_dataframe(
        "1JCl7-oZWUeGJwF87ggARxlxK8StAaKCFlu7CTfe1MXs", "투믹스(raw)", "A2:K"
    )
    .with_columns(
        pl.col(
            [
                col
                for col in internal_schema.keys()
                if col not in ["날짜", "구분1", "그룹"]
            ]
        ).str.replace_all(",", "")
    )
    .cast(internal_schema)
)

# Just in case when Toomics people forget to update their raw you know?
tmx_internal_max_date = tmx_internal_data.select(pl.col("날짜")).max().item()
logging.info(f"tmx_internal_max_date: {tmx_internal_max_date}")
tmx_data_for_join = tmx_data_for_join.filter(pl.col("날짜") <= tmx_internal_max_date)

# Finally joined...
joined = tmx_data_for_join.join(
    tmx_internal_data,
    on=["날짜", "구분1", "그룹"],
    how="left",
).sort("날짜")

# ======================================================================================
# Step 5. All done now really do upload away!
# ======================================================================================

# Uploading TMX internal compatible DF
gs.upload_dataframe(
    df=toomics_merged,
    sheet_key="1JCl7-oZWUeGJwF87ggARxlxK8StAaKCFlu7CTfe1MXs",
    sheet_name="raw",
    range=ut.dataframe_to_a1_address(toomics_merged, vertical_offset=1),
)

# Uploading Joined DF
gs.upload_dataframe(
    df=joined,
    sheet_key="1JCl7-oZWUeGJwF87ggARxlxK8StAaKCFlu7CTfe1MXs",
    sheet_name="raw_joined",
    range=ut.dataframe_to_a1_address(joined),
)
