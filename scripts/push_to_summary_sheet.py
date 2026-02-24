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
# LOAD SENTIMENT CLEAN
# =============================
sentiment_df = pd.read_csv(CLEAN_CSV)

sentiment_df["snapshot_time"] = pd.to_datetime(
    sentiment_df["snapshot_time"],
    errors="coerce"
)

# Chuẩn hóa về snapshot_date (tránh lệch giờ)
sentiment_df["snapshot_date"] = sentiment_df["snapshot_time"].dt.date

# =============================
# LOAD SUMMARY SHEET
# =============================
summary_data = sheet.get_all_records()
summary_df = pd.DataFrame(summary_data)

if summary_df.empty:
    raise ValueError("❌ Summary sheet is empty")

summary_df["snapshot_time"] = pd.to_datetime(
    summary_df["snapshot_time"],
    dayfirst=True,
    errors="coerce"
)

# Chuẩn hóa về snapshot_date
summary_df["snapshot_date"] = summary_df["snapshot_time"].dt.date

# =============================
# MAP GOLD TYPE → GOLD CODE
# =============================
# Loại bỏ dòng gold_type trống
summary_df = summary_df[summary_df["gold_type"].astype(str).str.strip() != ""]

summary_df["gold_code"] = summary_df["gold_type"].map(GOLD_NAME_TO_CODE)

# Kiểm tra mapping thực sự sai (không tính dòng trống)
missing_mask = summary_df["gold_code"].isna()

if missing_mask.any():
    missing = summary_df.loc[missing_mask, "gold_type"].unique()
    raise ValueError(f"❌ Missing gold mapping: {missing}")

# =============================
# MERGE (SAFE BY DATE + CODE)
# =============================
merged = pd.merge(
    summary_df,
    sentiment_df[
        [
            "snapshot_date",
            "gold_code",
            "news_volume",
            "sentiment_raw",
            "sentiment_score"
        ]
    ],
    on=["snapshot_date", "gold_code"],
    how="left"
)

# Nếu không có sentiment → trung tính
merged["news_volume"] = merged["news_volume"].fillna(0)
merged["sentiment_raw"] = merged["sentiment_raw"].fillna(0)
merged["sentiment_score"] = merged["sentiment_score"].fillna(0)

# =============================
# CLEAN UP TEMP COLUMNS
# =============================
merged = merged.drop(columns=["snapshot_date"])

# =============================
# PUSH BACK TO SHEET
# =============================

sheet.clear()

sheet.append_row(merged.columns.tolist())

sheet.append_rows(
    merged.astype(str).values.tolist(),
    value_input_option="USER_ENTERED"
)

print("✅ Summary sheet updated safely (merged by snapshot_date + gold_code)")
