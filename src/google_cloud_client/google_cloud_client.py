import polars as pl
from pathlib import Path
from yaspin import yaspin
import logging
import gspread
from google.oauth2.service_account import Credentials
import time


class GoogleCloudClient:
    def __init__(self, service_account_json: Path, scopes: list | None = None):
        """
        Initialize the GoogleCloudClient for accessing Google Cloud APIs.

        Parameters:
            service_account_json (Path): Path to the service account JSON credentials file.
            scopes (list[str], optional): List of OAuth2 scopes to authorize. If not provided, defaults to:
            [
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive",
            ]
        """
        self.scopes = scopes or [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]
        self.service_account_json = service_account_json

        try:
            self.creds = Credentials.from_service_account_file(
                self.service_account_json, scopes=self.scopes
            )
            self.googlesheet = GoogleSheetService(self.creds)
        except Exception as e:
            logging.error(f"Failed to initialize GoogleCloudClient: {e}")
            raise


class GoogleSheetService:
    def __init__(self, creds):
        self.creds = creds
        self.client = gspread.authorize(self.creds)

    def get_dataframe(
        self, sheet_key: str, sheet_name: str, range: str
    ) -> pl.DataFrame:
        """
        Fetches a range of tabular data from a Google Sheet and returns it as a Polars DataFrame.

        Parameters:
            sheet_key (str): The unique key of the Google Spreadsheet (from the URL).
            sheet_name (str): The name of the worksheet (tab) within the spreadsheet.
            range (str): The A1 notation string specifying the cell range to retrieve (e.g., "A1:K100").

        Returns:
            pl.DataFrame: A Polars DataFrame containing the sheet data, where the first row of the range is
                        treated as the header (column names) and the remaining rows as data.

        Raises:
            ValueError: If the specified sheet name is not found in the spreadsheet.
            Exception: For any other issues encountered during authentication or data retrieval.
        """

        with yaspin(color="blue") as spinner:
            try:
                spinner.text = "Authorizing gspread..."
                client = self.client

                spreadsheet = client.open_by_key(sheet_key)
                spreadsheet_name = spreadsheet.title

                spinner.text = f"'{spreadsheet_name}' opened"
                time.sleep(1)
                if sheet_name not in [ws.title for ws in spreadsheet.worksheets()]:
                    raise ValueError(
                        f"Sheet '{sheet_name}' not found in spreadsheet '{spreadsheet_name}'"
                    )

                sheet = spreadsheet.worksheet(sheet_name)

                # Step 2: Getting data
                spinner.text = f"Getting data at range {range}"
                output = sheet.get(range)

                # Step 3: Converting listed tabular data into `polar.DataFrame`
                header = output[0]
                rows = output[1:]

                df = pl.DataFrame(schema=header, data=rows, orient="row")

                spinner.text = f"DataFrame fetched from {spreadsheet_name} > '{sheet_name}' > '{range}'"
                spinner.ok("✅")
                return df

            except Exception as e:
                spinner.text = "Failed to fetch data"
                spinner.fail("💥")
                logging.error(f"Failed: {e}")
                raise

    def upload_dataframe(
        self, df: pl.DataFrame, sheet_key: str, sheet_name: str, range: str
    ):
        """
        Upload a Polars DataFrame to a specific Google Sheet range.

        Params:
            df         : pl.DataFrame — the data to upload
            sheet_key  : str          — the spreadsheet key
            sheet_name : str          — the target worksheet name
            range   : str          — A1-style range (e.g., "A1")
        """
        with yaspin(color="blue") as spinner:
            try:
                client = self.client

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
                spinner.text = f"Clearing data in range {range}"
                time.sleep(1)
                sheet.batch_clear([range])

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

                # Step 3: Upload df
                spinner.text = f"Uploading to range {range}"
                sheet.update(range_name=range, values=update_data)
                spinner.text = f"Uploaded DataFrame to '{spreadsheet_name}' > '{sheet_name}' > '{range}'"

                spinner.ok("✅")

            except Exception as e:
                spinner.fail("💥")
                spinner.text = f"Upload failed: {str(e)}"
                logging.error(f"Failed: {e}")
                raise
