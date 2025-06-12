import polars as pl
from pathlib import Path
import logging
import gspread
from google.oauth2.service_account import Credentials
from yaspin import yaspin
import time


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


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


def df_to_a1_range(
    df: pl.DataFrame,
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

    a1_address = f"{a1_start}{int_start}:{a1_end}{int_end}"

    return a1_address


def upload_df_to_gs(df: pl.DataFrame, sheet_key: str, sheet_name: str, a1_range: str):
    """
    Upload a Polars DataFrame to a specific Google Sheet range.

    Params:
        df         : pl.DataFrame — the data to upload
        sheet_key  : str          — the spreadsheet key
        sheet_name : str          — the target worksheet name
        a1_range   : str          — A1-style range (e.g., "A1")
    """
    spinner = None  # <== Prevent unbound local error

    try:
        with yaspin(color="blue") as spinner:
            scopes = [
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive",
            ]

            spinner.text = "Initializing credentials..."
            service_account_json = Path(
                "/Users/johnny/repos/polars-analytics/gcloud_credential.json"
            )

            creds = Credentials.from_service_account_file(
                service_account_json, scopes=scopes
            )
            client = gspread.authorize(creds)

            spreadsheet = client.open_by_key(sheet_key)
            spreadsheet_name = spreadsheet.title

            spinner.text = f"'{spreadsheet_name}' opened"
            time.sleep(1)
            if sheet_name not in [ws.title for ws in spreadsheet.worksheets()]:
                raise ValueError(
                    f"Sheet '{sheet_name}' not found in spreadsheet '{spreadsheet_name}'"
                )

            sheet = spreadsheet.worksheet(sheet_name)

            # Step 1: Clear existing data
            spinner.text = f"Clearing data in range {a1_range}"
            time.sleep(1)
            sheet.batch_clear([a1_range])

            def _polars_date_to_excel_serial(df: pl.DataFrame) -> pl.DataFrame:
                date_cols = [
                    col
                    for col, dtype in df.schema.items()
                    if isinstance(dtype, pl.Date)
                ]
                excel_unix_epoch_offset = 25569
                df = df.with_columns(
                    pl.col(date_cols).cast(pl.Int64) + excel_unix_epoch_offset
                )
                return df

            # Step 2: Prepare data
            df = _polars_date_to_excel_serial(df)
            header = [df.columns]
            rows = [list(row) for row in df.rows()]  # Convert tuples → lists
            update_data = header + rows

            # Step 3: Upload data
            spinner.text = f"Uploading to range {a1_range}"
            sheet.update(range_name=a1_range, values=update_data)
            spinner.text = f"Uploaded DataFrame to '{spreadsheet_name}' > sheet '{sheet_name}' > range '{a1_range}'"

            spinner.ok("✅")

    except Exception as e:
        spinner.fail("💥")
        spinner.text = f"Upload failed: {str(e)}"


if __name__ == "__main__":
    sheet_key_list = {
        "podl": "17-apAkDkg5diJVNeYYCYu7CcCFEn_iPSr3mGk3GWZS4",
        "refa": "19yc-6IIgh7UyFx2jepIFnSHiiGLr0if0nonr76kgHv8",
        "kcon": "12i4X3467bxW7Nc59ar3LsJ3tvdXD7nzFwxsLteB1bzY",
        "kahi": "12RBMaBfwqGYTx0H_Gn2VfDyDDu5-1JIjbqiNk2ZtBZA",
        "toomics": "1nRsC_vBdkJfn0WnM9Iu4NOJECn1G5UbSv0NRIbsgD4w",
    }

    df_path = Path(
        "/Users/johnny/repos/polars-analytics/data/apsl/proc/toomics_v2_2025-01-01–2025-06-11.csv"
    )

    df = pl.read_csv(df_path)

    upload_df_to_gs(df, sheet_key_list["podl"], "test", df_to_a1_range(df))


if __name__ == "__main__":
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
    kcon = apslETL(kcon_dir)

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
