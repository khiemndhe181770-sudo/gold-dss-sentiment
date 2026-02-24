import pandas as pd
import gspread
from google.oauth2.service_account import Credentials

# =============================
# CONFIG
# =============================
SPREADSHEET_ID = "1WW22O8SrCQPbjdAWJ2yUd5zLJOqL4-WmZHpRC0PLsQU"
SUMMARY_SHEET_NAME = "Data compilation THỬ NGHIỆM"

CLEAN_CSV = "data/sentiment_clean.csv"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# =============================
# GOLD NAME → CODE MAP
# =============================
GOLD_NAME_TO_CODE = {
    "Bao Tin 9999": "BT9999NTT",
    "Bao Tin SJC": "BTSJC",
    "DOJI Jewelry": "DOJINHTV",
    "DOJI Hanoi": "DOHNL",
    "DOJI HCM": "DOHCML",
    "PNJ 24K": "PQHN24NTT",
    "VN Gold SJC": "VNGSJC",
    "PNJ Hanoi": "PQHNVN",
    "SJC Ring": "SJ9999",
    "SJC 9999": "SJL1L10",
    "Viettin SJC": "VIETTINMSJC"
}

# =============================
# AUTH
# =============================
creds = Credentials.from_service_account_file(
    "gcp_service_account.json",
    scopes=SCOPES
)
gc = gspread.authorize(creds)

sheet = gc.open_by_key(SPREADSHEET_ID).worksheet(SUMMARY_SHEET_NAME)

# =============================
# LOAD DATA
# =============================

# Load sentiment clean
sentiment_df = pd.read_csv(CLEAN_CSV)
sentiment_df["snapshot_time"] = pd.to_datetime(sentiment_df["snapshot_time"])

# Load summary sheet
summary_data = sheet.get_all_records()
summary_df = pd.DataFrame(summary_data)

summary_df["snapshot_time"] = pd.to_datetime(
    summary_df["snapshot_time"],
    dayfirst=True,
    errors="coerce"
)

# =============================
# MAP GOLD TYPE → GOLD CODE
# =============================
summary_df["gold_code"] = summary_df["gold_type"].map(GOLD_NAME_TO_CODE)

if summary_df["gold_code"].isna().any():
    missing = summary_df[summary_df["gold_code"].isna()]["gold_type"].unique()
    raise ValueError(f"❌ Missing gold mapping: {missing}")

# =============================
# MERGE (SAFE)
# =============================
merged = pd.merge(
    summary_df,
    sentiment_df[
        [
            "snapshot_time",
            "gold_code",
            "news_volume",
            "sentiment_raw",
            "sentiment_score"
        ]
    ],
    on=["snapshot_time", "gold_code"],
    how="left"
)

# Nếu không có sentiment → điền 0
merged["news_volume"] = merged["news_volume"].fillna(0)
merged["sentiment_raw"] = merged["sentiment_raw"].fillna(0)
merged["sentiment_score"] = merged["sentiment_score"].fillna(0)

# =============================
# PUSH BACK TO SHEET
# =============================

sheet.clear()

sheet.append_row(merged.columns.tolist())
sheet.append_rows(
    merged.astype(str).values.tolist(),
    value_input_option="USER_ENTERED"
)

print("✅ Summary sheet updated safely without data drift")
