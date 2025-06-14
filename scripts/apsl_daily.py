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
toomics_raw_dir = apsl_dir / "raw" / "toomics"
podl_raw_dir = apsl_dir / "raw" / "podl"
kahi_raw_dir = apsl_dir / "raw" / "kahi"
refa_raw_dir = apsl_dir / "raw" / "refa"
kcon_raw_dir = apsl_dir / "raw" / "kcon"
processed_dir = apsl_dir / "proc"

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

kahi_meta_mapping = {
    "Day": "날짜",
    "Campaign name": "캠페인 명",
    "Ad Set Name": "광고 세트 명",
    "Ad name": "광고 명",
    "Gender": "성별",
    "Age": "연령",
    "Link (ad settings)": "링크(광고 설정)",
    "Amount spent (USD)": "지출 금액",
    "Impressions": "노출",
    "Frequency": "빈도",
    "Reach": "도달",
    "Clicks (all)": "클릭(전체)",
    "Link clicks": "링크 클릭",
    "Adds to cart": "장바구니에 담기",
    "Checkouts Initiated": "결제 시작",
    "Purchases": "구매",
    "Purchases conversion value": "구매 전환 값",
    "Video plays": "동영상 전체 재생",
    "Video plays at 25%": "동영상 25% 이상 재생",
    "Video plays at 50%": "동영상 50% 이상 재생",
    "Video plays at 75%": "동영상 75% 이상 재생",
    "Video plays at 100%": "동영상 100% 재생",
}

kahi_standard_schema = {
    "Source": pl.String,
    "날짜": pl.Date,
    "캠페인 명": pl.String,
    "광고 세트 명": pl.String,
    "광고 명": pl.String,
    "성별": pl.String,
    "연령": pl.String,
    "링크(광고 설정)": pl.String,
    "지출 금액": pl.Float64,
    "노출": pl.Int64,
    "빈도": pl.Float64,
    "도달": pl.Int64,
    "클릭(전체)": pl.Int64,
    "링크 클릭": pl.Int64,
    "장바구니에 담기": pl.Int64,
    "결제 시작": pl.Int64,
    "구매": pl.Int64,
    "구매 전환 값": pl.Float64,
    "동영상 전체 재생": pl.Int64,
    "동영상 25% 이상 재생": pl.Int64,
    "동영상 50% 이상 재생": pl.Int64,
    "동영상 75% 이상 재생": pl.Int64,
    "동영상 100% 재생": pl.Int64,
}

refa_meta_mapping = {
    "Day": "날짜",
    "Campaign name": "캠페인 명",
    "Ad Set Name": "광고 세트 명",
    "Ad name": "광고 명",
    "Gender": "성별",
    "Age": "연령",
    "Link (ad settings)": "링크(광고 설정)",
    "Amount spent (USD)": "지출 금액",
    "Impressions": "노출",
    "Frequency": "빈도",
    "Reach": "도달",
    "Clicks (all)": "클릭(전체)",
    "Link clicks": "링크 클릭",
    "Adds to cart": "장바구니에 담기",
    "Checkouts Initiated": "결제 시작",
    "Purchases": "구매",
    "Purchases conversion value": "구매 전환 값",
    "Video plays": "동영상 전체 재생",
    "Video plays at 25%": "동영상 25% 이상 재생",
    "Video plays at 50%": "동영상 50% 이상 재생",
    "Video plays at 75%": "동영상 75% 이상 재생",
    "Video plays at 100%": "동영상 100% 재생",
}

refa_standard_schema = {
    "Source": pl.String,
    "날짜": pl.Date,
    "캠페인 명": pl.String,
    "광고 세트 명": pl.String,
    "광고 명": pl.String,
    "성별": pl.String,
    "연령": pl.String,
    "링크(광고 설정)": pl.String,
    "지출 금액": pl.Float64,
    "노출": pl.Int64,
    "빈도": pl.Float64,
    "도달": pl.Int64,
    "클릭(전체)": pl.Int64,
    "링크 클릭": pl.Int64,
    "장바구니에 담기": pl.Int64,
    "결제 시작": pl.Int64,
    "구매": pl.Int64,
    "구매 전환 값": pl.Float64,
    "동영상 전체 재생": pl.Int64,
    "동영상 25% 이상 재생": pl.Int64,
    "동영상 50% 이상 재생": pl.Int64,
    "동영상 75% 이상 재생": pl.Int64,
    "동영상 100% 재생": pl.Int64,
}

kcon_meta_mapping = {
    "Day": "Day",
    "Campaign name": "Campaign name",
    "Ad Set Name": "Ad Set Name",
    "Ad name": "Ad name",
    "Gender": "Gender",
    "Age": "Age",
    "Amount spent (KRW)": "Amount spent (Raw)",
    "Currency": "Currency",
    "Impressions": "Impressions",
    "Clicks (all)": "Clicks (all)",
    "Link clicks": "Link clicks",
}

kcon_tiktok_mapping = {
    "By Day": "Day",
    "Campaign name": "Campaign name",
    "Ad group name": "Ad Set Name",
    "Ad name": "Ad name",
    "Gender": "Gender",
    "Age": "Age",
    "Cost": "Amount spent (Raw)",
    "Currency": "Currency",
    "Impressions": "Impressions",
    "Clicks (all)": "Clicks (all)",
    "Clicks (destination)": "Link clicks",
}

kcon_x_mapping = {
    "Time period": "Day",
    "Campaign name": "Campaign name",
    "Ad Group name": "Ad Set Name",
    "Ad name": "Ad name",
    "Spend": "Amount spent (Raw)",
    "Currency": "Currency",
    "Impressions": "Impressions",
    "Clicks": "Clicks (all)",
    "Link clicks": "Link clicks",
}

# Standardized schema for all data sources
kcon_standard_schema = {
    "Source": pl.String,
    "Day": pl.Date,
    "Campaign name": pl.String,
    "Ad Set Name": pl.String,
    "Ad name": pl.String,
    "Age": pl.String,
    "Gender": pl.String,
    "Amount spent (Raw)": pl.Float64,
    "Currency": pl.String,
    "Impressions": pl.Int64,
    "Clicks (all)": pl.Int64,
    "Link clicks": pl.Int64,
}

dfs = []
for f in toomics_raw_dir.glob("*.csv"):
    # 1) extract “Meta”, “X”, “TikTok”, etc.
    src = f.stem.split("_", 1)[0]
    # 2) read the CSV once
    df = pl.read_csv(f, infer_schema_length=0)
    # 3) tag with Source
    df = df.with_columns(pl.lit(src).alias("Source"))
    # 4) move Source to be the first column
    df = df.select(["Source"] + [c for c in df.columns if c != "Source"])
    dfs.append(df)

# 1) Separate into two lists by source
meta_frames = [df for df in dfs if df["Source"][0] == "Meta"]
x_frames = [df for df in dfs if df["Source"][0] == "X (Twitter)"]

# 2) Combine all Meta frames as-is
meta_combined: pl.DataFrame = pl.concat(meta_frames)
x_combined: pl.DataFrame = pl.concat(x_frames)

meta_cols = meta_combined.columns
missing = [col for col in meta_cols if col not in x_combined.columns]

x_combined = x_combined.with_columns(
    [
        pl.lit(None).alias(col)  # <-- one alias per column
        for col in missing
    ]
).select(meta_cols)

toomics_merged = pl.concat([meta_combined, x_combined])

toomics_merged = toomics_merged.with_columns(
    [
        pl.col("Spent")
        .str.replace_all("￦", "")
        .str.replace_all(",", "")
        .cast(pl.Float64)
        .alias("Spent"),
        pl.col("Impression")
        .str.replace_all(",", "")
        .cast(pl.Int64)
        .alias("Impression"),
        pl.col("Click").str.replace_all(",", "").cast(pl.Int64).alias("Click"),
        pl.col("First purchase")
        .str.replace_all(",", "")
        .str.replace_all('"', "")
        .cast(pl.Int64)
        .alias("First purchase"),
        pl.col("Purchase")
        .str.replace_all(",", "")
        .str.replace_all('"', "")
        .cast(pl.Int64)
        .alias("Purchase"),
        pl.col("First purchase")
        .str.replace_all('"', "")
        .cast(pl.Int64)
        .alias("Registration"),
        pl.col("Purchase value")
        .str.replace_all("￦", "")
        .str.replace_all(",", "")
        .cast(pl.Float64)
        .alias("Purchase value"),
        pl.col("First purchase value")
        .str.replace_all("￦", "")
        .str.replace_all(",", "")
        .cast(pl.Float64)
        .alias("First purchase value"),
        pl.col("Date").cast(pl.Date),
    ]
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


min_date = toomics_merged.select(pl.col("Date").min()).item()
max_date = toomics_merged.select(pl.col("Date").max()).item()

toomics_out = processed_dir / f"toomics_v2_{min_date}–{max_date}.csv"

logger.info(f"Exported to: {toomics_out}")

toomics = MultiSourceAdETL(toomics_raw_dir)
podl = MultiSourceAdETL(podl_raw_dir)
kahi = MultiSourceAdETL(kahi_raw_dir)
refa = MultiSourceAdETL(refa_raw_dir)
kcon = MultiSourceAdETL(kcon_raw_dir)

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

kahi_merged = (
    kahi.read_tabular_files()
    .assign_source()
    .standardize(
        standard_schema=kahi_standard_schema,
        meta_mapping=kahi_meta_mapping,
    )
    .merge_and_collect()
)

kahi_out = kahi.construct_file_name("kahi", kahi_merged)

refa_campaign_mapping = {
    "[ROAS] ASC_250108_ Product Mix_camp": "[MIX] ASC_250108_ Product Mix_camp",
    "[ROAS] ASC_250117 video camp 캠페인": "[M1] ASC_250117 video camp 캠페인",
    "[ROAS] ASC_New year Promotion_250109_camp 캠페인": "[M1] ASC_New year Promotion_250109_camp 캠페인",
}

refa_merged = (
    refa.read_tabular_files()
    .assign_source()
    .standardize(
        standard_schema=refa_standard_schema,
        meta_mapping=refa_meta_mapping,
    )
    .merge_and_collect()
).with_columns(pl.col("캠페인 명").replace(refa_campaign_mapping).alias("캠페인 명"))

refa_out = refa.construct_file_name("refa", refa_merged)

kcon_merged = (
    kcon.read_tabular_files()
    .assign_source()
    .clean_tiktok_remove_total()
    .standardize(
        standard_schema=kcon_standard_schema,
        meta_mapping=kcon_meta_mapping,
        tiktok_mapping=kcon_tiktok_mapping,
        x_mapping=kcon_x_mapping,
    )
    .merge_and_collect()
)

gcc()

kcon_out = kcon.construct_file_name("kcon", kcon_merged)

# Initializing and setting up Google Cloud service's gspread
gcloud_credential = Path("/Users/johnny/repos/polars-analytics/gcloud_credential.json")

gs = gcc(gcloud_credential).googlesheet

daily_exports = {
    "toomics": {
        "df": toomics_merged,
        "out": toomics_out,
        "sheet_key": "1JCl7-oZWUeGJwF87ggARxlxK8StAaKCFlu7CTfe1MXs",
        "sheet_name": "raw",
        "a1_range": ut.dataframe_to_a1_address(
            df=toomics_merged, vertical_offset=1, horizontal_offset=2
        ),
    },
    "podl": {
        "df": podl_merged,
        "out": podl_out,
        "sheet_key": "17-apAkDkg5diJVNeYYCYu7CcCFEn_iPSr3mGk3GWZS4",
        "sheet_name": "raw",
        "a1_range": ut.dataframe_to_a1_address(podl_merged),
    },
    "kahi": {
        "df": kahi_merged,
        "out": kahi_out,
        "sheet_key": "12RBMaBfwqGYTx0H_Gn2VfDyDDu5-1JIjbqiNk2ZtBZA",
        "sheet_name": "raw",
        "a1_range": ut.dataframe_to_a1_address(kahi_merged),
    },
    "refa": {
        "df": refa_merged,
        "out": refa_out,
        "sheet_key": "19yc-6IIgh7UyFx2jepIFnSHiiGLr0if0nonr76kgHv8",
        "sheet_name": "raw",
        "a1_range": ut.dataframe_to_a1_address(refa_merged),
    },
    "kcon": {
        "df": kcon_merged,
        "out": kcon_out,
        "sheet_key": "12i4X3467bxW7Nc59ar3LsJ3tvdXD7nzFwxsLteB1bzY",
        "sheet_name": "raw",
        "a1_range": ut.dataframe_to_a1_address(kcon_merged),
    },
}

for k, v in daily_exports.items():
    # Exported merged csvs to the computer
    df: pl.DataFrame = v["df"]
    out = processed_dir / v["out"]
    logger.info(f"{k} exported to {out}")
    df.write_csv(out, include_bom=True)

# Upload df to the sheet
gs.upload_dataframe(
    df=v["df"],
    sheet_key=v["sheet_key"],
    sheet_name=v["sheet_name"],
    a1_range=v["a1_range"],
)
