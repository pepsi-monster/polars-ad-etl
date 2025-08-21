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
