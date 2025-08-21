import polars as pl
from pathlib import Path
import plotly.express as px
import plotly.io as pio
import utils.utils as ut
from google_cloud_client.google_cloud_client import GoogleCloudClient as gcc

pio.renderers.default = "browser"

raw_dir = Path(__file__).parent.parent / "data" / "apsl" / "raw" / "kcon_region"

file_path = [f for f in raw_dir.glob("*.csv")]

df = pl.read_csv(file_path[0], infer_schema_length=None)


df_schema = {
    "Day": pl.Date,
    "Campaign name": pl.String,
    "Ad Set Name": pl.String,
    "Ad name": pl.String,
    "Region": pl.String,
    "Amount spent (KRW)": pl.Float64,
    "Currency": pl.String,
    "Impressions": pl.Int64,
    "Clicks (all)": pl.Int64,
    "Link clicks": pl.Int64,
    "Reporting starts": pl.String,
    "Reporting ends": pl.String,
}

locations = {
    "USA": {
        "Washington, District of Columbia": "DC",
        "Kansas": "KS",
        "Missouri": "MO",
        "Pennsylvania": "PA",
        "Colorado": "CO",
        "Tennessee": "TN",
        "Hawaii": "HI",
        "Arkansas": "AR",
        "South Carolina": "SC",
        "Connecticut": "CT",
        "Virginia": "VA",
        "Vermont": "VT",
        "California": "CA",
        "Maryland": "MD",
        "Delaware": "DE",
        "Ohio": "OH",
        "New Mexico": "NM",
        "Illinois": "IL",
        "Idaho": "ID",
        "Mississippi": "MS",
        "Wyoming": "WY",
        "Minnesota": "MN",
        "Washington": "WA",
        "Wisconsin": "WI",
        "New Jersey": "NJ",
        "West Virginia": "WV",
        "Nevada": "NV",
        "Texas": "TX",
        "Arizona": "AZ",
        "Utah": "UT",
        "Iowa": "IA",
        "New Hampshire": "NH",
        "Oklahoma": "OK",
        "Louisiana": "LA",
        "Kentucky": "KY",
        "Maine": "ME",
        "Montana": "MT",
        "Alabama": "AL",
        "Massachusetts": "MA",
        "Michigan": "MI",
        "North Carolina": "NC",
        "New York": "NY",
        "Oregon": "OR",
        "Alaska": "AK",
        "Nebraska": "NE",
        "Rhode Island": "RI",
        "South Dakota": "SD",
        "Indiana": "IN",
        "North Dakota": "ND",
        "Florida": "FL",
        "Georgia": "GA",
    },
    "Japan": {
        "Tochigi Prefecture": "Tochigi Prefecture",
        "Fukui Prefecture": "Fukui Prefecture",
        "Kagoshima Prefecture": "Kagoshima Prefecture",
        "Miyazaki Prefecture": "Miyazaki Prefecture",
        "Tottori Prefecture": "Tottori Prefecture",
        "Gifu Prefecture": "Gifu Prefecture",
        "Shimane Prefecture": "Shimane Prefecture",
        "Yamagata Prefecture": "Yamagata Prefecture",
        "Kagawa Prefecture": "Kagawa Prefecture",
        "Yamaguchi Prefecture": "Yamaguchi Prefecture",
        "Nagano Prefecture": "Nagano Prefecture",
        "Chiba Prefecture": "Chiba Prefecture",
        "Fukushima Prefecture": "Fukushima Prefecture",
        "Osaka Prefecture": "Osaka Prefecture",
        "Fukuoka Prefecture": "Fukuoka Prefecture",
        "Akita Prefecture": "Akita Prefecture",
        "Tokyo": "Tokyo",
        "Wakayama Prefecture": "Wakayama Prefecture",
        "Shizuoka Prefecture": "Shizuoka Prefecture",
        "Kanagawa Prefecture": "Kanagawa Prefecture",
        "Ibaraki Prefecture": "Ibaraki Prefecture",
        "Shiga Prefecture": "Shiga Prefecture",
        "Miyagi Prefecture": "Miyagi Prefecture",
        "Toyama Prefecture": "Toyama Prefecture",
        "Hiroshima Prefecture": "Hiroshima Prefecture",
        "Okinawa Prefecture": "Okinawa Prefecture",
        "Yamanashi Prefecture": "Yamanashi Prefecture",
        "Aichi Prefecture": "Aichi Prefecture",
        "Aomori Prefecture": "Aomori Prefecture",
        "Kumamoto Prefecture": "Kumamoto Prefecture",
        "Kyoto Prefecture": "Kyoto Prefecture",
        "Iwate Prefecture": "Iwate Prefecture",
        "Ishikawa Prefecture": "Ishikawa Prefecture",
        "Okayama Prefecture": "Okayama Prefecture",
        "Ehime Prefecture": "Ehime Prefecture",
        "Tokushima Prefecture": "Tokushima Prefecture",
        "Mie Prefecture": "Mie Prefecture",
        "Hyōgo Prefecture": "Hyōgo Prefecture",
        "Niigata Prefecture": "Niigata Prefecture",
        "Saga Prefecture": "Saga Prefecture",
        "Hokkaido": "Hokkaido",
        "Nara Prefecture": "Nara Prefecture",
        "Saitama Prefecture": "Saitama Prefecture",
        "Nagasaki Prefecture": "Nagasaki Prefecture",
        "Ōita Prefecture": "Ōita Prefecture",
        "Kōchi Prefecture": "Kōchi Prefecture",
        "Gunma Prefecture": "Gunma Prefecture",
    },
    "Ethiopia": {
        "Addis Ababa": "Addis Ababa",
    },
    "Jordan": {
        "Amman Governorate": "Amman Governorate",
    },
    "India": {
        "Punjab": "Punjab",
        "Gujarat": "Gujarat",
    },
    "Unknown": {
        "Unknown": "Unknown",
    },
}

region_to_country = {
    region: country for country, reg_dict in locations.items() for region in reg_dict
}

us_region_to_abbreviation = locations.get("USA")

df = df.with_columns(
    pl.col("Region").replace(region_to_country).alias("Country")
).with_columns(
    pl.col("Region")
    .replace_strict(us_region_to_abbreviation, default="")
    .alias("State codes")
)

df_us_only = (
    df.filter(pl.col("Country") == "USA")
    .group_by(["State codes", "Region"])
    .agg(pl.col("Amount spent (KRW)").sum())
).sort(pl.col("Amount spent (KRW)"), descending=True)


df_us_only_total = df_us_only.select(pl.col("Amount spent (KRW)").sum()).item()


df_us_only = df_us_only.with_columns(
    (pl.col("Amount spent (KRW)") / pl.lit(df_us_only_total)).alias("Share of spend")
)


fig = px.choropleth(
    df_us_only.to_pandas(),
    locations="State codes",  # your two‐letter codes
    locationmode="USA-states",
    color="Share of spend",
    scope="usa",
    hover_name="Region",  # show full state name on hover
    color_continuous_scale="Blues",
    title="KRW share of spend by U.S. State",
)
fig.show()

gcloud_credential = Path("/Users/johnny/repos/polars-analytics/gcloud_credential.json")
gs = gcc(gcloud_credential).googlesheet

min_date = df.select(pl.col("Day")).min().item()
max_date = df.select(pl.col("Day")).max().item()

date_mark = pl.DataFrame({f"기간: {min_date}–{max_date}": ""})

gs.upload_dataframe(
    date_mark,
    "12i4X3467bxW7Nc59ar3LsJ3tvdXD7nzFwxsLteB1bzY",
    "kcon_region_meta_only",
    "A1",
)


gs.upload_dataframe(
    df,
    "12i4X3467bxW7Nc59ar3LsJ3tvdXD7nzFwxsLteB1bzY",
    "kcon_region_meta_only",
    ut.dataframe_to_a1_address(df, vertical_offset=1),
)
