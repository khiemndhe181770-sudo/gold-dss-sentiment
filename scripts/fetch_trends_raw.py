# scripts/fetch_trends_raw.py
from pytrends.request import TrendReq
import pandas as pd
import time
from datetime import datetime

# -----------------------------
# CONFIG
# -----------------------------
KEYWORDS = [
    # ======================
    # GLOBAL / BASELINE
    # ======================
    "gold price",
    "giá vàng",
    "giá vàng thế giới",
    "XAUUSD",

    # ======================
    # INVESTMENT SENTIMENT
    # ======================
    "đầu tư vàng",
    "có nên mua vàng",
    "xu hướng giá vàng",

    # ======================
    # GOLD TYPE (SJC / 9999)
    # ======================
    "vàng SJC",
    "SJC 9999",
    "vàng 9999",
    "vàng 24k",

    # ======================
    # BRAND AWARENESS (RAW)
    # ======================
    "vàng DOJI",
    "vàng PNJ",
    "vàng PNJ 24K"
]

GEO = "VN"
TIMEFRAME = "now 7-d"   # an toàn nhất
SLEEP_TIME = 90
SLEEP_SEC = 30   # chống block

# -----------------------------
# INIT PYTRENDS
# -----------------------------
pytrends = TrendReq(
    hl="vi-VN",
    tz=420,
    timeout=(10, 30)
)

raw_frames = []

for kw in KEYWORDS:
    try:
        print(f"Fetching: {kw}")

        pytrends.build_payload(
            kw_list=[kw],
            timeframe=TIMEFRAME,
            geo=""   # ❗ BẮT BUỘC: để trống
        )

        df = pytrends.interest_over_time()

        if df.empty:
            print(f"⚠️ No data for {kw}")
            continue

        df = df.reset_index()
        df = df.rename(columns={kw: "trend_score"})
        df["keyword"] = kw

        raw_frames.append(
            df[["date", "keyword", "trend_score"]]
        )

        time.sleep(SLEEP_SEC)

    except Exception as e:
        print(f"❌ Error with {kw}: {e}")
        time.sleep(SLEEP_SEC * 2)

# =============================
# SAVE RAW DSS TABLE
# =============================
if raw_frames:
    raw_df = pd.concat(raw_frames, ignore_index=True)

    raw_df["snapshot_time"] = datetime.utcnow()
    raw_df["data_source"] = "Google Trends"
    raw_df["region"] = "Vietnam"

    raw_df.to_csv("data/raw_sentiment.csv", index=False, encoding="utf-8-sig")
    print("✅ RAW sentiment data saved")

else:
    print("❌ No data collected")