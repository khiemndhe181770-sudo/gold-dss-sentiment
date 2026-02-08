import pandas as pd
import gspread
from google.oauth2.service_account import Credentials

# =============================
# CONFIG
# =============================
SPREADSHEET_ID = "1DGvajxGMPyiAViJpb0MlSu08DUEgK3fFV2FGoKPwa58"
CLEAN_SHEET_NAME = "clean"
CSV_PATH = "data/sentiment_clean.csv"

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


# =============================
# AUTH
# =============================
creds = Credentials.from_service_account_file(
    "gcp_service_account.json",
    scopes=SCOPES
)
gc = gspread.authorize(creds)

sheet = gc.open_by_key(SPREADSHEET_ID).worksheet(CLEAN_SHEET_NAME)

# =============================
# LOAD CLEAN CSV
# =============================
df = pd.read_csv(CSV_PATH)

# =============================
# DSS HEADER (CHUẨN)
# =============================
header = [
    "snapshot_id",
    "snapshot_time",
    "gold_code",
    "gold_group",
    "news_volume",
    "sentiment_raw",
    "sentiment_score"
]

missing = set(header) - set(df.columns)
if missing:
    raise ValueError(f"❌ Missing CLEAN columns: {missing}")

# =============================
# ENSURE HEADER ON SHEET
# =============================
if not sheet.get_all_values():
    sheet.append_row(header)

# =============================
# PUSH DATA
# =============================
rows = df[header].values.tolist()

sheet.append_rows(
    rows,
    value_input_option="USER_ENTERED"
)

print(f"✅ Pushed {len(rows)} CLEAN rows to Google Sheets")