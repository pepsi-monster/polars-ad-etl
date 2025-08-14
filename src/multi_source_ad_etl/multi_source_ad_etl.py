import polars as pl
from pathlib import Path
import logging


class MultiSourceAdETL:
    def __init__(
        self,
        raw_dir: Path,
        source_criteria: dict[str, set[str]],
        rename_mappings: dict[str, dict[str, str]],
        standard_schema: dict[str, pl.DataType],
    ):
        self.raw_dir = raw_dir
        self.dfs: list[pl.DataFrame] = []
        self.source_criteria = source_criteria
        self.rename_mappings = rename_mappings
        self.standard_schema = standard_schema
        self._validate_source_criteria()
        self._validate_alignment()
        self._validate_schema_coverage()

    def _validate_alignment(self):
        crit_keys = set(self.source_criteria.keys())
        map_keys = set(self.rename_mappings.keys())

        missing = sorted(crit_keys - map_keys)
        extra = sorted(map_keys - crit_keys)
        if missing or extra:
            msgs = []
            if missing:
                msgs.append(f"Missing rename_mappings for sources: {missing}")
            if extra:
                msgs.append(f"Mappings provided for non-detectable sources: {extra}")
            raise ValueError(" | ".join(msgs))

    def _validate_source_criteria(self):
        criteria = self.source_criteria
        col_to_keys = {}

        for src, cols in criteria.items():
            for col in cols:
                if col not in col_to_keys:
                    col_to_keys[col] = []
                col_to_keys[col].append(src)

        for col, srcs in col_to_keys.items():  # srcs is already the list
            if len(srcs) > 1:
                raise ValueError(
                    f"Column '{col}' is used in multiple sources: {', '.join(srcs)}"
                )

    def _validate_schema_coverage(self):
        """Ensure all mapping targets are defined in the standard schema."""
        schema_cols = set(self.standard_schema.keys())
        bad = []
        for src, mp in self.rename_mappings.items():
            targets = set(mp.values())
            missing_in_schema = sorted(targets - schema_cols)
            if missing_in_schema:
                bad.append(f"{src}: {missing_in_schema}")
        if bad:
            raise ValueError(
                "Mapping targets not present in standard_schema -> " + " | ".join(bad)
            )

    def read_tabular_files(self):
        for f in self.raw_dir.iterdir():
            suffix = f.suffix.lower()
            if suffix == ".csv":
                self.dfs.append(pl.read_csv(f, infer_schema_length=None))
            elif suffix == ".xlsx":
                self.dfs.append(pl.read_excel(f, infer_schema_length=None))
        return self

    def _detect_source(self, df: pl.DataFrame) -> str:
        # Default criteria if none provided
        criteria = self.source_criteria

        df_cols = set(df.columns)

        for src, required_cols in criteria.items():
            if set(required_cols) <= df_cols:
                return src

        raise ValueError(f"Source: 'Unknown' assigned (columns: {df.columns})")

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
    ):
        updated_dfs = []
        mapping_lookup = self.rename_mappings
        standard_schema = self.standard_schema

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
    kcon_mapping = {
        "Meta": {
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
        },
        "TikTok": {
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
        },
        "X (Twitter)": {
            "Time period": "Day",
            "Campaign name": "Campaign name",
            "Ad Group name": "Ad Set Name",
            "Ad name": "Ad name",
            "Spend": "Amount spent (Raw)",
            "Currency": "Currency",
            "Impressions": "Impressions",
            "Clicks": "Clicks (all)",
            "Link clicks": "Link clicks",
        },
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

    kcon_source_criteria = {
        "Meta": {"Campaign name", "Day"},
        "TikTok": {"By Day", "Cost"},
        "X (Twitter)": {"Time period", "Spend"},
    }

    kcon_dir = Path("/Users/johnny/repos/polars-analytics/data/apsl/raw/kcon")
    kcon = MultiSourceAdETL(
        raw_dir=kcon_dir,
        source_criteria=kcon_source_criteria,
        rename_mappings=kcon_mapping,
        standard_schema=kcon_standard_schema,
    )

    kcon_merged = (
        kcon.read_tabular_files()
        .assign_source()
        .clean_tiktok_remove_total()
        .standardize()
        .merge_and_collect()
    )
    print(kcon_merged)
