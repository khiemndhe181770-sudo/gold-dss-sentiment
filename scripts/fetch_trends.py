from pytrends.request import TrendReq
import pandas as pd
import time
from datetime import datetime

# -----------------------------
# CONFIG
# -----------------------------
KEYWORDS = [
    "gold price"
]

GEO = "VN"
TIMEFRAME = 'today 30-d'
SLEEP_TIME = 90
SLEEP_SEC = 20   # chống block

# -----------------------------
# INIT PYTRENDS
# -----------------------------
pytrends = TrendReq(
    hl='en-US',
    tz=420,
    timeout=(10, 25),
    requests_args={
        'headers': {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'
        }
    }
)

all_data = []

# -----------------------------
# FETCH LOOP (SAFE MODE)
# -----------------------------
for kw in KEYWORDS:
    try:
        print(f"Fetching: {kw}")

        pytrends.build_payload(
            kw_list=[kw],
            timeframe=TIMEFRAME,
            geo=GEO
        )

        df = pytrends.interest_over_time()

        if df.empty:
            print(f"⚠️ No data for {kw}")
            continue

        df = df.reset_index()
        df["keyword"] = kw
        df = df.rename(columns={kw: "trend_score"})
        df = df[["date", "keyword", "trend_score"]]

        all_data.append(df)

        time.sleep(SLEEP_SEC)

    except Exception as e:
        print(f"❌ Error with {kw}: {e}")
        time.sleep(SLEEP_SEC * 2)

# -----------------------------
# COMBINE RAW DATA
# -----------------------------
if all_data:
    raw_df = pd.concat(all_data, ignore_index=True)

    raw_df["snapshot_time"] = datetime.utcnow()
    raw_df["data_source"] = "Google Trends"
    raw_df["geo"] = GEO

    raw_df.to_csv("data/raw_sentiment.csv", index=False)

    print("✅ RAW sentiment data saved")

else:
    print("❌ No data collected")
