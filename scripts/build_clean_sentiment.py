import pandas as pd
import hashlib
from datetime import datetime, time
import pytz
# =================================================
# 1. CONFIG – GOLD CODES & MAPPING
# =================================================

SUPPORTED_GOLD_CODES = [
    "XAUUSD",
    "SJL1L10",
    "SJ9999",
    "DOHNL",
    "DOHCML",
    "DOJINHTV",
    "BTSJC",
    "BT9999NTT",
    "PQHNVN",
    "PQHN24NTT",
    "VNGSJC",
    "VIETTINMSJC"
]

GOLD_CODE_TO_GROUP = {
    "XAUUSD": "WORLD",

    "SJL1L10": "SJC",
    "SJ9999": "9999",
    "BTSJC": "SJC",
    "VNGSJC": "SJC",
    "VIETTINMSJC": "SJC",

    "BT9999NTT": "9999",
    "PQHN24NTT": "9999",

    "DOHNL": "GENERAL",
    "DOHCML": "GENERAL",
    "DOJINHTV": "GENERAL",
    "PQHNVN": "GENERAL"
}

# =================================================
# 2. MAP KEYWORD → GOLD GROUP
# =================================================

def map_gold_group(keyword: str) -> str:
    kw = keyword.lower()
    if "xau" in kw or "thế giới" in kw or "gold price" in kw:
        return "WORLD"
    if "sjc" in kw:
        return "SJC"
    if "9999" in kw or "24k" in kw:
        return "9999"
    return "GENERAL"

# =================================================
# 3. LOAD RAW DATA
# =================================================

df = pd.read_csv("data/raw_sentiment.csv")

# snapshot_time → UTC → giờ Việt Nam
df["snapshot_time"] = pd.to_datetime(
    df["snapshot_time"],
    utc=True,
    errors="coerce"
).dt.tz_convert("Asia/Ho_Chi_Minh")

vn_tz = pytz.timezone("Asia/Ho_Chi_Minh")

df["snapshot_date"] = df["snapshot_time"].dt.date

df["snapshot_time"] = df["snapshot_date"].apply(
    lambda d: vn_tz.localize(
        datetime.combine(d, time(9, 30))
    )
)

# MAP gold_group
df["gold_group"] = df["keyword"].apply(map_gold_group)

# =================================================
# 4. AGGREGATE → sentiment_raw, news_volume
# =================================================

grouped = (
    df.groupby(["snapshot_time", "gold_group"])
      .agg(
          news_volume=("trend_score", "count"),
          sentiment_raw=("trend_score", "mean")
      )
      .reset_index()
)

# =================================================
# 5. DSS-CORRECT SENTIMENT SCORE (PERCENTILE)
# =================================================
# ✔ Chuẩn hóa theo lịch sử
# ✔ Không giả định phân phối
# ✔ Phù hợp risk / DSS

grouped["sentiment_percentile"] = (
    grouped
    .groupby("gold_group")["sentiment_raw"]
    .rank(pct=True)
)

grouped["sentiment_score"] = (
    grouped["sentiment_percentile"] - 0.5
) * 2

# =================================================
# 6. EXPAND → FULL 12 GOLD CODES / SNAPSHOT
# =================================================

expanded_rows = []

for snapshot_time in grouped["snapshot_time"].unique():
    snap = grouped[grouped["snapshot_time"] == snapshot_time]

    for gold_code in SUPPORTED_GOLD_CODES:
        group = GOLD_CODE_TO_GROUP.get(gold_code, "GENERAL")
        row = snap[snap["gold_group"] == group]

        if not row.empty:
            base = row.iloc[0]
            news_volume = int(base["news_volume"])
            sentiment_raw = float(base["sentiment_raw"])
            sentiment_score = float(base["sentiment_score"])
        else:
            # Không có dữ liệu → trung tính
            news_volume = 0
            sentiment_raw = 0.0
            sentiment_score = 0.0

        expanded_rows.append({
            "snapshot_time": snapshot_time,
            "gold_code": gold_code,
            "gold_group": group,
            "news_volume": news_volume,
            "sentiment_raw": sentiment_raw,
            "sentiment_score": sentiment_score
        })

final_clean = pd.DataFrame(expanded_rows)

# =================================================
# 7. SNAPSHOT ID
# =================================================

final_clean["snapshot_id"] = final_clean["snapshot_time"].astype(str).apply(
    lambda x: hashlib.md5(x.encode()).hexdigest()[:10]
)

# =================================================
# 8. SAVE CLEAN DSS TABLE
# =================================================

final_clean = final_clean[
    [
        "snapshot_id",
        "snapshot_time",
        "gold_code",
        "gold_group",
        "news_volume",
        "sentiment_raw",
        "sentiment_score"
    ]
]

final_clean.to_csv(
    "data/sentiment_clean.csv",
    index=False,
    encoding="utf-8-sig"
)

print("✅ CLEAN sentiment built – DSS percentile normalized")