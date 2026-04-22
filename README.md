# Indian Market Data Pipeline

Automated daily pipeline that collects OHLCV data for **~150 Nifty 200 stocks + Nifty Index** across 1m, 5m, and 1h timeframes. Runs every weekday at 10:30 PM IST automatically. No paid API, no server needed.

---

## How it works

Every weekday at 10:30 PM IST, GitHub Actions automatically:
- Fetches fresh data from Yahoo Finance for all stocks
- Appends new rows to your CSV files (no duplicates)
- Validates data quality and logs any issues
- Commits everything back to your repo

You do nothing. Just download when you need it.

---

## Folder structure

```
your-repo/
│
├── pipeline.py
├── requirements.txt
├── .github/
│   └── workflows/
│       └── pipeline.yml
│
├── data/
│   ├── 1m/
│   │   ├── master_1m_2026_03.csv
│   │   ├── master_1m_2026_04.csv
│   │   └── ...
│   ├── 5m/
│   │   ├── master_5m_2026.csv
│   │   └── master_5m_2027.csv
│   ├── master_1h.csv
│   ├── backup_master_1h.csv
│   ├── validation_report.csv
│   └── skipped_tickers.csv
│
└── logs/
    └── pipeline_20260328.log
```

1m = one file per month | 5m = one file per year | 1h = single file always

---

## Data columns

Every CSV file has these 7 columns:

| Column | Example | Description |
|--------|---------|-------------|
| `datetime` | `2026-03-28 09:15:00` | Candle open time (IST) |
| `ticker` | `RELIANCE` | Stock symbol |
| `open` | `1423.50` | Opening price |
| `high` | `1431.20` | Highest price |
| `low` | `1419.80` | Lowest price |
| `close` | `1428.75` | Closing price |
| `volume` | `284500` | Shares traded |

---

## How to download your data

**Option 1 — Artifacts zip (recommended)**
1. Go to your repo → **Actions** tab
2. Click the latest green tick run
3. Scroll to the bottom → click **market-data-XXX**
4. Zip downloads with all CSV files inside

> Artifacts expire after 30 days. For older data, use Option 2.

**Option 2 — Download individual files**
1. Go to your repo → `data/` folder → `1m/` or `5m/`
2. Click any CSV file
3. Click the **Download raw file** button (top right)

> If you see "Sorry, we can't show this file" — that is fine, it is just too large to preview. The download button still works.

---

## How to use the data for backtesting

Install pandas if you haven't:
```bash
pip install pandas
```

**Load all 1-minute data (all months at once):**
```python
import pandas as pd
import glob

files = sorted(glob.glob("data/1m/master_1m_*.csv"))
df = pd.concat([pd.read_csv(f, parse_dates=["datetime"]) for f in files])
```

This glob pattern picks up every monthly file automatically — works whether you have 1 file or 24.

**Load 5-minute or 1-hour data:**
```python
df_5m = pd.read_csv("data/5m/master_5m_2026.csv", parse_dates=["datetime"])
df_1h = pd.read_csv("data/master_1h.csv", parse_dates=["datetime"])
```

**Filter for one stock and date range:**
```python
reliance = df_1h[df_1h["ticker"] == "RELIANCE"].copy()
reliance = reliance[
    (reliance["datetime"] >= "2026-01-01") &
    (reliance["datetime"] <= "2026-12-31")
].sort_values("datetime").reset_index(drop=True)
```

**Simple moving average crossover backtest:**
```python
stock = df_1h[df_1h["ticker"] == "RELIANCE"].copy().sort_values("datetime").reset_index(drop=True)

stock["sma20"] = stock["close"].rolling(20).mean()
stock["sma50"] = stock["close"].rolling(50).mean()

stock["signal"] = 0
stock.loc[stock["sma20"] > stock["sma50"], "signal"] = 1    # buy
stock.loc[stock["sma20"] < stock["sma50"], "signal"] = -1   # sell
```

---

## Data growth over time



| Time Running | 1m Files | 5m Files | 1h Files | Estimated Total Storage |
|--------------|-----------|-----------|-----------|--------------------------|
| 1 Month | ~1 file (~35–50 MB) | ~1 file (~20–30 MB) | ~5–10 MB | ~60–90 MB |
| 2 Months | ~2 files (~35–50 MB each) | ~1 file (~40–60 MB) | ~10–15 MB | ~140–190 MB |
| 3 Months | ~3 files (~35–50 MB each) | ~1 file (~60–90 MB) | ~15–20 MB | ~220–300 MB |
| 6 Months | ~6 files (~35–50 MB each) | ~1 file (~120–180 MB) | ~30–40 MB | ~450–650 MB |
| 9 Months | ~9 files (~35–50 MB each) | ~1 file (~180–270 MB) | ~45–60 MB | ~650–900 MB |
| 1 Year | ~12 files (~35–50 MB each) | ~1 yearly file (~250–350 MB) | ~60–80 MB | ~850 MB – 1.1 GB |
| 2 Years | ~24 files (~35–50 MB each) | ~2 yearly files (~250–350 MB each) | ~120–160 MB | ~1.7 – 2.2 GB |

GitHub free plan allows 1 GB — you have room for 2+ years. New monthly and yearly files are created automatically when a new month or year starts.

---

## Schedule

| When | What happens |
|------|-------------|
| Mon–Fri 10:30 PM IST | Pipeline runs automatically |
| Saturday and Sunday | Skipped (NSE closed) |
| Any time | Go to Actions → **Run workflow** to trigger manually |

---

## Troubleshooting

| Problem | Fix |
|---------|-----|
| Workflow shows red cross | Click the run to see which step failed |
| File shows "can't preview" | Use the Download raw file button — still works |
| A stock has no data | Check `skipped_tickers.csv` — may be delisted or renamed |
| Artifact not found | Expired after 30 days — download from `data/` folder directly |

---

## Setup guide (for new users)

1. Create a GitHub account at [github.com](https://github.com)
2. Create a new **private** repository
3. Upload `pipeline.py`, `requirements.txt`, and `.github/workflows/pipeline.yml`
4. Go to **Settings → Actions → General → Workflow permissions** → select **Read and write permissions** → Save
5. Go to **Actions → Run workflow** to do a first test run
6. Green tick = done. Runs every weekday automatically from now on.

---

## Notes

- **1m data** — Yahoo Finance only provides last 7 days of 1m data. Pipeline runs daily so nothing is missed as long as it keeps running.
- **Free** — No API key, no paid service. Uses GitHub's free 2,000 Actions minutes/month (each run takes ~20 min).
- **Data quality** — Suitable for backtesting and research. Not institutional grade data.

---

*Python · yfinance · pandas · GitHub Actions*
