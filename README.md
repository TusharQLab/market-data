# 🇮🇳 Indian Market Data Pipeline

Automated daily pipeline that fetches 1m / 5m / 1h OHLCV data for **Nifty 200 + Nifty Index**,
validates it, and stores everything in clean master CSV files — all via **GitHub Actions** (free).

---

## 📁 Project Structure

```
your-repo/
├── pipeline.py                  ← main script
├── requirements.txt
├── .github/
│   └── workflows/
│       └── pipeline.yml         ← GitHub Actions automation
├── data/                        ← auto-created on first run
│   ├── master_1m.csv
│   ├── master_5m.csv
│   ├── master_1h.csv
│   ├── backup_master_1m.csv
│   └── validation_report.csv
└── logs/                        ← daily log files
```

---

## ⚡ Quick Setup (5 minutes)

### Step 1 — Create a GitHub repository

1. Go to [github.com/new](https://github.com/new)
2. Create a **private** repo (recommended for financial data)
3. Copy the repo URL

### Step 2 — Upload files

Upload these 3 files to the root of your repo:
- `pipeline.py`
- `requirements.txt`
- `.github/workflows/pipeline.yml`

Or clone and push:
```bash
git clone https://github.com/YOUR_USERNAME/YOUR_REPO.git
cd YOUR_REPO
# copy the files here
git add .
git commit -m "initial setup"
git push
```

### Step 3 — Enable GitHub Actions write permission

1. Go to your repo → **Settings** → **Actions** → **General**
2. Scroll to **Workflow permissions**
3. Select **"Read and write permissions"**
4. Click **Save**

### Step 4 — Run it!

- **Automatic**: Runs every weekday at 22:30 IST (after NSE close)
- **Manual**: Go to **Actions** tab → Select workflow → Click **"Run workflow"**

---

## 📊 Output Files

| File | Description |
|------|-------------|
| `data/master_1m.csv` | 1-minute OHLCV (last 7 days, appended daily) |
| `data/master_5m.csv` | 5-minute OHLCV (last 60 days, appended daily) |
| `data/master_1h.csv` | 1-hour OHLCV (last 365 days, appended daily) |
| `data/backup_master_1m.csv` | Backup of previous 1m master |
| `data/validation_report.csv` | Any OHLC / volume mismatches found |

**Columns in every file:** `datetime, ticker, open, high, low, close, volume`

---

## 🔍 How Validation Works

Every run:
1. Fetches **actual 5m data** from Yahoo Finance API
2. **Generates 5m candles** by resampling the 1m data
3. Compares them — checks for:
   - Missing timestamps
   - OHLC differences > 1% (configurable)
   - Volume differences > 5% (configurable)
4. Logs all issues to `validation_report.csv`

To adjust tolerance, edit these lines in `pipeline.py`:
```python
OHLC_TOLERANCE   = 0.01   # 1%
VOLUME_TOLERANCE = 0.05   # 5%
```

---

## 🖥 Run Locally

```bash
# 1. Create a virtual environment (optional but recommended)
python -m venv venv
source venv/bin/activate      # Windows: venv\Scripts\activate

# 2. Install dependencies
pip install -r requirements.txt

# 3. Run
python pipeline.py
```

---

## ⚙️ Customization

### Change the run schedule
Edit this line in `.github/workflows/pipeline.yml`:
```yaml
- cron: "0 17 * * 1-5"   # 17:00 UTC = 22:30 IST, Mon–Fri
```
Use [crontab.guru](https://crontab.guru) to build your schedule.

### Add / remove tickers
Edit the `NIFTY200_TICKERS` list in `pipeline.py`.

### Change lookback window
Edit the `DAYS` dict in `pipeline.py`:
```python
DAYS = {"1m": 7, "5m": 60, "1h": 365}
```
> Note: Yahoo Finance limits 1m data to last **7 days** and 5m to last **60 days**.

---

## ⚠️ Important Notes

- **Data source**: Yahoo Finance (free, no API key needed)
- **1m data**: Only last 7 days available per Yahoo Finance limits
- **Weekends**: Pipeline skips weekends automatically (NSE is closed)
- **Data storage**: Files are committed back to your GitHub repo after each run
- **GitHub free tier**: 2,000 Actions minutes/month — this pipeline uses ~15–20 min/run

---

## 🐛 Troubleshooting

| Problem | Fix |
|---------|-----|
| `403` push error | Enable write permissions (Step 3 above) |
| Empty data files | Yahoo Finance may throttle; try running again later |
| Missing tickers | Some tickers may be delisted; check Yahoo Finance directly |
| Workflow not running | Check Actions tab is enabled in repo settings |

Check `logs/pipeline_YYYYMMDD.log` for detailed error messages.
