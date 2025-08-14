import polars as pl
from pathlib import Path
import logging


class MultiSourceAdETL:
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

    def _detect_source(
        self, df: pl.DataFrame, source_criteria: dict[str, set[str]] | None = None
    ) -> str:
        # Default criteria if none provided
        if source_criteria is None:
            source_criteria = {
                "Meta": {"Campaign name", "Day"},
                "TikTok": {"By Day", "Cost"},
                "X (Twitter)": {"Time period", "Spend"},
            }
        else:
            col_to_keys = {}

            for src, cols in source_criteria.items():
                for col in cols:
                    if col not in col_to_keys:
                        col_to_keys[col] = []
                    col_to_keys[col].append(src)

            for col, srcs in col_to_keys.items():  # srcs is already the list
                if len(srcs) > 1:
                    raise ValueError(
                        f"Column '{col}' is used in multiple sources: {', '.join(srcs)}"
                    )

        df_cols = set(df.columns)

        for src, required_cols in source_criteria.items():
            if required_cols <= df_cols:
                return src

        logging.warning(f"Source: 'Unknown' assigned (columns: {df.columns})")
        return "Unknown"

    def assign_source(self, source_criteria: dict[str, set[str]] | None = None):
        """
        Detect and assign the advertising platform source for each DataFrame in `self.dfs`.

        This method uses `_detect_source` to identify which platform (e.g., Meta, TikTok, X/Twitter)
        a given DataFrame belongs to based on the presence of certain identifying columns.

        Args:
            source_criteria (dict[str, set[str]], optional):
                A mapping of platform names to the set of column names required to identify them.
                - Keys are source names (strings).
                - Values are sets of column names (exact matches, case-sensitive) that must
                all be present in the DataFrame for that source to be assigned.
                If None, the method uses the default criteria defined in `_detect_source`.

        Returns:
            Self, with each DataFrame in `self.dfs` updated to include a new
            `"Source"` column as the first column.

        Usage:
            # Using defaults:
            etl.assign_source()

            # Custom criteria:
            criteria = {
                "Meta": {"Campaign name", "Day"},
                "TikTok": {"By Day", "Cost"},
                "X (Twitter)": {"Time period", "Spend"},
                "Google": {"Stuff", "Thing"},
            }
            etl.assign_source(criteria)

        Notes:
            - Column matching is strict: names in `source_criteria` must match the DataFrame's
            column names exactly.
            - The first matching source in `source_criteria` order is assigned; if multiple
            platforms could match, the earlier one in the dict wins.
            - If no match is found, `_detect_source` returns `"Unknown"`.
            - Existing `"Source"` columns will be replaced.
        """
        updated_dfs = []

        for df in self.dfs:
            src = self._detect_source(df, source_criteria)

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
            if src == "X (Twitter)" and df.schema["Average frequency"] == pl.String:
                df = df.with_columns(
                    pl.when(pl.col("Average frequency") == "-")
                    .then(pl.lit(0))
                    .otherwise(pl.col("Average frequency"))
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
                df.with_columns(
                    [
                        pl.lit(None).alias(col)
                        for col in standard_schema.keys()
                        if col not in df.columns
                    ]
                )
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
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )
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
        "Amount spent (Raw)": pl.String,
        "Currency": pl.String,
        "Impressions": pl.Int64,
        "Clicks (all)": pl.Int64,
        "Link clicks": pl.Int64,
    }

    kcon_dir = Path("/Users/johnny/repos/polars-analytics/data/apsl/raw/kcon")
    kcon = MultiSourceAdETL(kcon_dir)

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
    print(kcon_merged)
