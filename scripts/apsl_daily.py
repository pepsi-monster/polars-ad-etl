from pathlib import Path
from plETL.apslETL import apslETL
import logging
import polars as pl

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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

toomics = apslETL(toomics_raw_dir)
podl = apslETL(podl_raw_dir)
kahi = apslETL(kahi_raw_dir)
refa = apslETL(refa_raw_dir)
kcon = apslETL(kcon_raw_dir)


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

toomics_out = toomics.construct_file_name("toomics", toomics_merged)

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

refa_merged = (
    refa.read_tabular_files()
    .assign_source()
    .standardize(
        standard_schema=refa_standard_schema,
        meta_mapping=refa_meta_mapping,
    )
    .merge_and_collect()
)

refa_out = refa.construct_file_name("refa", refa_merged)

daily_exports = {}
daily_exports = {
    "toomics": {"df": toomics_merged, "out": toomics_out},
    "podl": {"df": podl_merged, "out": podl_out},
    "kahi": {"df": kahi_merged, "out": kahi_out},
    "refa": {"df": refa_merged, "out": refa_out},
}
for client, v in daily_exports.items():
    df: pl.DataFrame = v["df"]
    out = processed_dir / v["out"]
    logger.info(f"{client} exported to {out}")
    df.write_csv(out, include_bom=True)
