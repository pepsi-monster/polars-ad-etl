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
like_eat_raw_dir = data_dir / "raw" / "like_eat"
processed_dir = data_dir / "proc"

# Create folders if they don't exist
data_dir.mkdir(parents=True, exist_ok=True)
like_eat_raw_dir.mkdir(parents=True, exist_ok=True)
processed_dir.mkdir(parents=True, exist_ok=True)

like_eat_mapping = {
    "Meta_naver": {
        "일": "일",
        "캠페인 이름": "캠페인 이름",
        "광고 세트 이름": "광고 세트 이름",
        "광고 이름": "광고 이름",
        "성": "성",
        "연령": "연령",
        "웹사이트 URL": "웹사이트 URL",
        "지출 금액 (KRW)": "지출 금액 (KRW)",
        "노출": "노출",
        "빈도": "빈도",
        "도달": "도달",
        "링크 클릭": "링크 클릭",
        "공유 항목이 포함된 장바구니에 담기": "장바구니 담기",
        "공유 항목이 포함된 구매": "구매",
        "공유 항목의 구매 전환값": "구매 전환값",
        "동영상 25% 재생": "동영상 25% 재생",
        "동영상 50% 재생": "동영상 50% 재생",
        "동영상 75% 재생": "동영상 75% 재생",
        "동영상 95% 재생": "동영상 95% 재생",
        "동영상 100% 재생": "동영상 100% 재생",
        "동영상 재생": "동영상 재생",
        "ThruPlay": "ThruPlay",
    },
    "Naver_GFA": {
        "기간": "일",
        "애셋 그룹 이름": "광고 세트 이름",
        "캠페인 이름": "캠페인 이름",
        "총 비용": "지출 금액 (KRW)",
        "노출": "노출",
        "클릭": "링크 클릭",
        "구매완료수": "구매",
        "장바구니 담기수": "장바구니 담기",
        "구매완료 전환 매출액": "구매 전환값",
    },
}

# pl.Int64는 정수, Pl.String은 문자타입, Pl.Float64는 소수, Pl.date는 날짜로 위 맵핑한 우측 단어를 활용하여 추가하면 됨
like_eat_standard_schema = {
    "Source": pl.String,
    "일": pl.Date,
    "캠페인 이름": pl.String,
    "광고 세트 이름": pl.String,
    "광고 이름": pl.String,
    "성": pl.String,
    "연령": pl.String,
    "웹사이트 URL": pl.String,
    "지출 금액 (KRW)": pl.Float64,
    "노출": pl.Int64,
    "빈도": pl.Float64,
    "도달": pl.Int64,
    "링크 클릭": pl.Int64,
    "장바구니 담기": pl.Int64,
    "구매": pl.Int64,
    "구매 전환값": pl.Float64,
    "동영상 25% 재생": pl.Int64,
    "동영상 50% 재생": pl.Int64,
    "동영상 75% 재생": pl.Int64,
    "동영상 95% 재생": pl.Int64,
    "동영상 100% 재생": pl.Int64,
    "동영상 재생": pl.Int64,
    "ThruPlay": pl.Int64,
}

like_eat_src_criteria = {
    "Meta_naver": {"공유 항목이 포함된 구매", "공유 항목이 포함된 장바구니에 담기"},
    "Naver_GFA": {
        "연령 및 성별",
        "애셋 그룹 이름",
    },
}

cleaners = {
    "Naver_GFA": [cln.clean_naver_gfa_age_gender, cln.clean_naver_gfa_date],
}

like_eat = MultiSourceAdETL(
    raw_dir=like_eat_raw_dir,
    source_criteria=like_eat_src_criteria,
    rename_mappings=like_eat_mapping,
    standard_schema=like_eat_standard_schema,
    cleaning_functions=cleaners,
)

like_eat_merged = (
    like_eat.read_tabular_files()
    .capitalize_col_names()
    .assign_source()
    .clean_dataframes()
    .standardize_dataframes()
    .merge_and_collect()
)

like_eat_out = processed_dir / ut.make_date_filename("like_eat", like_eat_merged)

daily_exports = {
    "like_eat": {
        "export": True,  # True means that export to the proc dir
        "upload": True,  # True means that upload to the sheet
        "df": like_eat_merged,
        "sheet_key": "1qS-g-grvB1VyzVv3NUgVzEMSM8VWJOKD0_ceC3RyTsI",
        "sheet_name": "raw",  # 👋 Don't forget to change this part!!!!!
        "out": like_eat_out,
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
