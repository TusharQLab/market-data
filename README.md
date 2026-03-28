# Indian Market Data Pipeline

An automated system that collects daily stock market data for **~150 Nifty 200 stocks + Nifty Index** across three timeframes — 1 minute, 5 minute, and 1 hour — and stores it in clean CSV files inside your GitHub repository. Runs automatically every weekday evening. No paid API, no server, no manual work.

---

## What this does

Every weekday at **10:30 PM IST** (after NSE closes at 3:30 PM), GitHub Actions automatically:

1. Fetches fresh OHLCV data for all stocks from Yahoo Finance (free, no API key needed)
2. Appends it to your master data files — no duplicates, sorted by date
3. Validates the data quality — checks for missing candles and price mismatches
4. Saves a backup of the 1h file
5. Commits everything back to your repo
6. Uploads a downloadable zip of all files (the "Artifact")

You don't need to do anything. Just check back and download whenever you want.

---

## Folder structure

\`\`\`
your-repo/
├── pipeline.py
├── requirements.txt
├── .github/workflows/pipeline.yml
│
├── data/
│   ├── 1m/
│   │   ├── master_1m_2026_03.csv      ← March 2026 1-minute data
│   │   ├── master_1m_2026_04.csv      ← April 2026 1-minute data
│   │   └── ...                        ← one file per month, forever
│   ├── 5m/
│   │   ├── master_5m_2026.csv         ← all of 2026 5-minute data
│   │   └── master_5m_2027.csv         ← new file auto-created Jan 2027
│   ├── master_1h.csv
│   ├── backup_master_1h.csv
│   ├── validation_report.csv
│   └── skipped_tickers.csv
│
└── logs/
    └── pipeline_20260328.log
\`\`\`

---

## Data format

Every CSV file has the same 7 columns:

| Column | Type | Example | Description |
|--------|------|---------|-------------|
| \`datetime\` | datetime | \`2026-03-28 09:15:00\` | Candle open time (IST) |
| \`ticker\` | text | \`RELIANCE\` | Stock symbol |
| \`open\` | float | \`1423.50\` | Opening price |
| \`high\` | float | \`1431.20\` | Highest price in candle |
| \`low\` | float | \`1419.80\` | Lowest price in candle |
| \`close\` | float | \`1428.75\` | Closing price |
| \`volume\` | integer | \`284500\` | Shares traded |

---

## How much data you will collect

| Time | 1m files | 5m files | 1h file | Total repo |
|------|----------|----------|---------|------------|
| 1 month | 1 file ~5 MB | 1 file ~10 MB | ~3 MB | ~20 MB |
| 6 months | 6 files ~5 MB each | 1 file ~60 MB | ~18 MB | ~110 MB |
| 1 year | 12 files ~5 MB each | 1 file ~120 MB | ~35 MB | ~215 MB |
| 2 years | 24 files ~5 MB each | 2 files ~120 MB each | ~70 MB | ~410 MB |

GitHub free plan allows 1 GB per repo — you have room for 2+ years easily.

### Why files are split this way

- **1m → one file per month** — most rows, kept small so GitHub can preview and Excel opens instantly
- **5m → one file per year** — less dense, yearly file stays manageable
- **1h → one file forever** — sparse enough to never need splitting

New files are created automatically when a new month or year starts.

---

## How to download your data

### Option 1 — Artifacts zip (easiest, gets everything)

1. Go to your repo → click **Actions** tab
2. Click the latest green ✓ run
3. Scroll to the bottom → under **Artifacts** click **market-data-XXX**
4. Zip downloads — unzip to get all CSVs

> Artifacts expire after 30 days. Download before they expire, or use Option 2.

### Option 2 — Direct file download

1. Go to your repo → click `data/` folder
2. Navigate into `1m/` or `5m/`
3. Click any CSV file
4. Click the **Download raw file** button (top right)

> If you see "Sorry, we can't show this file" — that just means it's too large to preview. The download button still works fine.

---

## How to use the data for backtesting

### Install pandas

\`\`\`bash
pip install pandas
\`\`\`

### Load all 1-minute data (all months at once)

\`\`\`python
import pandas as pd
import glob

files = sorted(glob.glob("data/1m/master_1m_*.csv"))
df_1m = pd.concat([pd.read_csv(f, parse_dates=["datetime"]) for f in files])
print(f"Total rows: {len(df_1m):,}")
\`\`\`

The \`glob\` pattern works whether you have 1 file or 24 files — you never need to update this code.

### Load 5-minute or 1-hour data

\`\`\`python
df_5m = pd.read_csv("data/5m/master_5m_2026.csv", parse_dates=["datetime"])
df_1h = pd.read_csv("data/master_1h.csv", parse_dates=["datetime"])
\`\`\`

### Filter for one stock and date range

\`\`\`python
reliance = df_1h[df_1h["ticker"] == "RELIANCE"].copy()
reliance = reliance.sort_values("datetime").reset_index(drop=True)

# Filter date range
reliance = reliance[
    (reliance["datetime"] >= "2026-01-01") &
    (reliance["datetime"] <= "2026-03-31")
]
\`\`\`

### See all available tickers

\`\`\`python
print(sorted(df_1h["ticker"].unique()))
\`\`\`

### Simple moving average crossover backtest

\`\`\`python
df = pd.read_csv("data/master_1h.csv", parse_dates=["datetime"])
stock = df[df["ticker"] == "RELIANCE"].copy().sort_values("datetime").reset_index(drop=True)

stock["sma20"] = stock["close"].rolling(20).mean()
stock["sma50"] = stock["close"].rolling(50).mean()

stock["signal"] = 0
stock.loc[stock["sma20"] > stock["sma50"], "signal"] = 1   # buy
stock.loc[stock["sma20"] < stock["sma50"], "signal"] = -1  # sell

print(stock[["datetime", "close", "sma20", "sma50", "signal"]].tail(10))
\`\`\`

---

## Automation schedule

| When | What happens |
|------|-------------|
| Mon–Fri 10:30 PM IST | Pipeline runs, fetches data, saves to repo |
| Saturday–Sunday | No run (NSE closed) |
| NSE holidays | Run happens, fetches 0 rows, no error |
| Anytime | Go to Actions → Run workflow to trigger manually |

To change the time, edit this line in \`.github/workflows/pipeline.yml\`:
\`\`\`yaml
- cron: "0 17 * * 1-5"   # 17:00 UTC = 22:30 IST
\`\`\`
Use [crontab.guru](https://crontab.guru) to build a custom schedule.

---

## Data validation

Every run the pipeline:

1. Resamples 1m data → generates 5m and 1h candles
2. Compares those against the candles fetched directly from Yahoo Finance
3. Flags any row where OHLC differs by more than 1% or volume by more than 5%
4. Saves issues to \`data/validation_report.csv\` (last 30 days only)

Tickers that fail to fetch are logged in \`data/skipped_tickers.csv\` with the reason.

---

## Stocks covered

All Nifty 200 stocks available on Yahoo Finance, plus \`^NSEI\` (Nifty 50 Index). Includes all Nifty 50 large caps, Nifty Next 50, and selected midcaps. Check \`skipped_tickers.csv\` to see which ones were unavailable on any given day.

---

## Troubleshooting

| Problem | Fix |
|---------|-----|
| Workflow shows red ✗ | Click the run to see which step failed |
| File shows "can't preview" | Normal for large files — use Download raw file button |
| A stock has no data | Check \`skipped_tickers.csv\` — may be delisted or renamed |
| Artifact not found | Artifacts expire after 30 days — download directly from \`data/\` folder |
| Push failed with 100 MB error | A log file grew too large — delete it from the repo manually |

---

## Fresh setup guide

1. Create a GitHub account at [github.com](https://github.com)
2. Create a new private repository
3. Upload \`pipeline.py\`, \`requirements.txt\`, and \`.github/workflows/pipeline.yml\`
4. Go to **Settings → Actions → General → Workflow permissions** → select **Read and write permissions** → Save
5. Go to **Actions** → **Run workflow** to test
6. Green ✓ = done. Runs every weekday automatically from now on.

---

## Running locally

\`\`\`bash
pip install -r requirements.txt
python pipeline.py
\`\`\`

---

## Important notes

- **Yahoo Finance 1m limit** — Only last 7 days available. Pipeline runs daily so nothing is missed as long as it keeps running.
- **Yahoo Finance 5m limit** — Last 59 days available. Fetched fresh each run, duplicates removed automatically.
- **GitHub Actions free tier** — 2,000 minutes/month. Each run uses ~20 minutes = ~100 runs possible. More than enough for 22 weekday runs per month.
- **Data quality** — Yahoo Finance is free but not institutional grade. Suitable for backtesting and research, not for live trading order placement.
- **Keep repo private** — Recommended so your collected data stays yours.

---

*Built with Python · yfinance · pandas · GitHub Actions · No paid services · No API keys*
