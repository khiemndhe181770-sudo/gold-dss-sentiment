# scripts/push_raw_to_sheet.py
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime

# =============================
# CONFIG
# =============================
SPREADSHEET_ID = "1DGvajxGMPyiAViJpb0MlSu08DUEgK3fFV2FGoKPwa58"
RAW_SHEET_NAME = "raw"
CSV_PATH = "data/raw_sentiment.csv"

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# =============================
# AUTH
# =============================
creds = Credentials.from_service_account_file(
    "gcp_service_account.json",
    scopes=SCOPES
)
gc = gspread.authorize(creds)

sheet = gc.open_by_key(SPREADSHEET_ID).worksheet(RAW_SHEET_NAME)

# =============================
# LOAD RAW CSV
# =============================
df = pd.read_csv(CSV_PATH)

# =============================
# DSS SANITY CHECK
# =============================
required_cols = {
    "date",
    "keyword",
    "trend_score",
    "snapshot_time",
    "data_source",
    "region"
}

missing = required_cols - set(df.columns)
if missing:
    raise ValueError(f"❌ Missing DSS columns: {missing}")

# =============================
# FORMAT VALUES (SAFE)
# =============================
df["snapshot_time"] = pd.to_datetime(df["snapshot_time"]).dt.strftime(
    "%Y-%m-%d %H:%M:%S"
)

rows = df[
    [
        "date",
        "keyword",
        "trend_score",
        "snapshot_time",
        "data_source",
        "region"
    ]
].values.tolist()

# =============================
# APPEND TO RAW SHEET
# =============================
sheet.append_rows(
    rows,
    value_input_option="USER_ENTERED"
)

print(f"✅ Pushed {len(rows)} RAW rows to Google Sheets")
