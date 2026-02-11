import polars as pl


def clean_x_avg_frequency(df: pl.DataFrame) -> pl.DataFrame:
    if df.schema["Average frequency"] == pl.String:
        df = df.with_columns(
            pl.when(pl.col("Average frequency") == "-")
            .then(pl.lit(0))
            .otherwise(pl.col("Average frequency"))
            .alias("Average frequency")
        )
    return df


def remove_tiktok_total_row(df: pl.DataFrame) -> pl.DataFrame:
    total_col = df.columns[1]
    df = df.remove(pl.col(total_col).str.starts_with("Total"))
    return df


def strip_tiktok_mp4_suffix(df: pl.DataFrame) -> pl.DataFrame:
    df = df.with_columns(pl.col("Ad name").str.strip_suffix(".mp4"))
    return df


def clean_naver_gfa_age_gender(df: pl.DataFrame) -> pl.DataFrame:
    """
    Naver GFA 전용 cleaner

    - `연령 및 성별` → `연령`, `성`
    - 연령:
        * `세` 제거
        * `~`/`–`/`—` → `-`
        * `이상` → `+`
        * `연령모름` → `unknown`
        * Naver age bucket 유지 (rebucketing ❌)
    - 성:
        * 남자/남성 → male
        * 여자/여성 → female
        * 성별모름 → unknown
    """

    s = (
        pl.col("연령 및 성별")
        .cast(pl.Utf8)
        .str.strip_chars()
        .str.replace_all(r"\s+", " ")
    )

    # ---------- AGE ----------
    s_age = s.str.replace_all(r"[~–—]", "-").str.replace_all(r"\s*세\s*", "")

    age_range = s_age.str.extract(r"(\d{1,2})\s*-\s*(\d{1,2})", 0).str.replace_all(
        r"\s*-\s*", "-"
    )

    age_plus = s_age.str.extract(r"(\d{1,2})\s*이상", 1)

    age = (
        pl.when(s.str.contains("연령모름"))
        .then(pl.lit("unknown"))
        .when(age_range.is_not_null())
        .then(age_range)
        .when(age_plus.is_not_null())
        .then(age_plus + pl.lit("+"))
        .otherwise(pl.lit("unknown"))
    )

    # ---------- GENDER ----------
    gender = (
        pl.when(s.str.contains("성별모름"))
        .then(pl.lit("unknown"))
        .when(s.str.contains("남자|남성"))
        .then(pl.lit("male"))
        .when(s.str.contains("여자|여성"))
        .then(pl.lit("female"))
        .otherwise(pl.lit("unknown"))
    )

    return df.with_columns(
        [
            age.alias("연령"),
            gender.alias("성"),
        ]
    ).drop("연령 및 성별")


def clean_naver_gfa_date(df: pl.DataFrame) -> pl.DataFrame:
    """
    Naver GFA 전용 날짜 cleaner

    Raw:  기간 = "2026.02.09."
    Goal: ISO date string "2026-02-09"
    - 컬럼명 변경 ❌
    - ISO 포맷으로만 정규화
    """

    return df.with_columns(
        pl.col("기간")
        .cast(pl.Utf8)
        .str.strip_chars()
        .str.strip_suffix(".")
        .str.replace_all(r"\.", "-")
        .alias("기간")
    )
