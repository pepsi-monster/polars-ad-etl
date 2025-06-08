import polars as pl
from pathlib import Path
import logging


class apslETL:
    def __init__(self, raw_dir: Path):
        self.raw_dir = raw_dir
        self.dfs: list[pl.DataFrame] = []

    def read_tabular_files(self):
        for f in self.raw_dir.iterdir():
            suffix = f.suffix.lower()
            if suffix == ".csv":
                self.dfs.append(pl.read_csv(f, infer_schema_length=None))
            elif suffix == ".xlsx":
                self.dfs.append(pl.read_excel(f, infer_schema_length=None))
        return self

    def _detect_source(self, df: pl.DataFrame) -> str:
        source_criteria = {
            "Meta": {"Campaign name", "Day"},
            "TikTok": {"By Day", "Cost"},
            "X (Twitter)": {"Time period", "Clicks"},
        }

        df_cols = set(df.columns)

        for src, required_cols in source_criteria.items():
            if required_cols <= df_cols:
                return src

        logging.warning(f"Source: 'Unknown' assigned (columns: {df.columns})")
        return "Unknown"

    def assign_source(self):
        updated_dfs = []

        for df in self.dfs:
            src = self._detect_source(df)

            df = df.with_columns(pl.lit(src).alias("Source")).select(
                ["Source"] + [col for col in df.columns if col != "Source"]
            )

            updated_dfs.append(df)

        self.dfs = updated_dfs
        return self

    def clean_x_avg_frequency(self):
        updated_dfs = []
        for df in self.dfs:
            src = df["Source"][0]
            if src == "X (Twitter)":
                avg_frequency_dtype = df.schema["Average frequency"]
                if avg_frequency_dtype == pl.String:
                    df = df.with_columns(
                        pl.col("Average frequency")
                        .replace_strict("-", "0")
                        .alias("Average frequency")
                    )
            updated_dfs.append(df)
        self.dfs = updated_dfs
        return self

    def clean_tiktok_remove_total(self):
        updated_dfs = []
        for df in self.dfs:
            src = df["Source"][0]

            if src == "TikTok":
                total_col = df.columns[1]
                df = df.remove(pl.col(total_col).str.starts_with("Total"))
            updated_dfs.append(df)
        self.dfs = updated_dfs
        return self

    def clean_tiktok_strip_mp4_suffix(self):
        updated_dfs = []
        for df in self.dfs:
            src = df["Source"][0]

            if src == "TikTok":
                df = df.with_columns(pl.col("Ad name").str.strip_suffix(".mp4"))
            updated_dfs.append(df)
        self.dfs = updated_dfs
        return self

    def standardize(
        self,
        standard_schema: dict,
        meta_mapping: dict = None,
        tiktok_mapping: dict = None,
        x_mapping: dict = None,
    ):
        updated_dfs = []
        mapping_lookup = {
            "Meta": meta_mapping,
            "TikTok": tiktok_mapping,
            "X (Twitter)": x_mapping,
        }

        for df in self.dfs:
            src = df["Source"][0]

            mapping = mapping_lookup.get(src)

            if mapping is None:
                raise ValueError(f"Mapping required for source: {src}")

            df = df.rename(mapping)
            # filling missing col and and converting 'Day' to `polars.Date`
            df = (
                df.with_columns([
                    pl.lit(None).alias(col)
                    for col in standard_schema.keys()
                    if col not in df.columns
                ])
                .select(standard_schema.keys())
                .cast(standard_schema)
            )
            updated_dfs.append(df)
        self.dfs = updated_dfs
        return self

    def construct_file_name(self, identifier: str, df: pl.DataFrame):
        date_cols = [col for col, dtype in df.schema.items() if dtype == pl.Date]
        first_date_col = date_cols[0] if date_cols else None

        min_date = df.select(pl.col(first_date_col)).min().item()
        max_date = df.select(pl.col(first_date_col)).max().item()
        file_name = f"{identifier}_{min_date}–{max_date}.csv"

        return file_name

    def merge_and_collect(self):
        merged: pl.DataFrame = pl.concat(self.dfs)
        logging.info(f"{len(self.dfs)} file(s) have been merged")
        return merged


if __name__ == "__main__":
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

    logger = logging.getLogger(__name__)
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s: %(message)s"
    )

    raw_dir = Path(__file__).parent.parent.parent / "data" / "apsl" / "raw" / "podl"
    proc_dir = Path(__file__).parent.parent.parent / "data" / "apsl" / "proc"

    podl = apslETL(raw_dir)

    merged = (
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
    out = proc_dir / podl.construct_file_name("podl", merged)
    logging.info(f"Exported to {out}")
    merged.write_csv(out)
