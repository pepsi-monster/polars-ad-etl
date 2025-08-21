from typing import Literal
import polars as pl


def make_date_filename(prefix: str, df: pl.DataFrame) -> str:
    """
    Create a filename with a date range from the first Date column in a DataFrame.

    Format: "{prefix}_{min_date}–{max_date}.csv"

    Raises
    ------
    ValueError
        If no Date column exists in the DataFrame.
    """
    date_cols = [col for col, dtype in df.schema.items() if dtype == pl.Date]

    if not date_cols:
        raise ValueError(f"Date col no found in {df}")

    first_date_col = date_cols[0]
    min_date = df.select(pl.col(first_date_col)).min().item()
    max_date = df.select(pl.col(first_date_col)).max().item()

    return f"{prefix}_{min_date}–{max_date}.csv"


def df_to_a1(
    df: pl.DataFrame,
    range_mode: Literal["column_range", "full_range"] = "full_range",
    vertical_offset: int | None = None,
    horizontal_offset: int | None = None,
):
    v_offset = vertical_offset or 0
    h_offset = horizontal_offset or 0

    df_shape = df.shape

    df_length = df_shape[0] + 1  # Including header row
    df_width = df_shape[1]

    def _int_to_bijective_base_26(n: int) -> str:
        s = ""
        while n > 0:
            n, r = divmod(n - 1, 26)
            s = chr(65 + r) + s
        return s

    a1_start = _int_to_bijective_base_26(1 + h_offset)
    int_start = 1 + v_offset
    a1_end = _int_to_bijective_base_26(df_width + h_offset)
    int_end = df_length + v_offset

    column_range = f"{a1_start}:{a1_end}"
    full_range = f"{a1_start}{int_start}:{a1_end}{int_end}"

    range = column_range if range_mode == "column_range" else full_range

    return range


if __name__ == "__main__":
    sample_data = [
        {
            "Campaign": "Meta_Summer2025",
            "Day": "2025-08-01",
            "Impressions": 12000,
            "Clicks": 350,
            "Spend": 78.5,
        },
        {
            "Campaign": "TikTok_BackToSchool",
            "Day": "2025-08-01",
            "Impressions": 18000,
            "Clicks": 500,
            "Spend": 92.0,
        },
        {
            "Campaign": "Google_Search_Ads",
            "Day": "2025-08-01",
            "Impressions": 9000,
            "Clicks": 260,
            "Spend": 45.2,
        },
        {
            "Campaign": "Pinterest_AutumnIdeas",
            "Day": "2025-08-01",
            "Impressions": 7500,
            "Clicks": 150,
            "Spend": 20.0,
        },
        {
            "Campaign": "X_TwitterPromo",
            "Day": "2025-08-01",
            "Impressions": 5000,
            "Clicks": 120,
            "Spend": 10.5,
        },
    ]

    df = pl.DataFrame(sample_data)

    print(df_to_a1(df, range_mode="full_range"))
    print(df_to_a1(df, range_mode="column_range"))
