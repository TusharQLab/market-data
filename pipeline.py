"""
Indian Market Data Pipeline v3
New in v3:
  - Files split by YYYY_MM (month) for 1m data  → each file ~3-5 MB max
  - Files split by YYYY    (year)  for 5m data  → each file ~8-10 MB max
  - 1h data stays in one file (it's always small)
  - Old big master files automatically cleaned up
  - GitHub can preview every file, Excel opens everything instantly
"""

import shutil
import logging
import warnings
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import yfinance as yf

warnings.filterwarnings("ignore")

# ─── Config ────────────────────────────────────────────────────────────────────

DATA_DIR  = Path("data")
LOG_DIR   = Path("logs")

# 1h stays as one file — it's always small (< 5 MB)
FILE_1H            = DATA_DIR / "master_1h.csv"
BACKUP_1H          = DATA_DIR / "backup_master_1h.csv"
VALIDATION_REPORT  = DATA_DIR / "validation_report.csv"
SKIPPED_LOG        = DATA_DIR / "skipped_tickers.csv"

OHLC_TOLERANCE     = 0.01   # 1%
VOLUME_TOLERANCE   = 0.05   # 5%

COLUMNS    = ["datetime", "ticker", "open", "high", "low", "close", "volume"]
DAYS_LIMIT = {"1m": 7, "5m": 59, "1h": 720}

# ─── Split file naming ─────────────────────────────────────────────────────────
#
#  1m  →  data/1m/master_1m_2026_03.csv   (one file per month)
#  5m  →  data/5m/master_5m_2026.csv      (one file per year)
#  1h  →  data/master_1h.csv              (single file, always small)
#
DIR_1M = DATA_DIR / "1m"
DIR_5M = DATA_DIR / "5m"

def path_1m(dt: datetime) -> Path:
    return DIR_1M / f"master_1m_{dt.strftime('%Y_%m')}.csv"

def path_5m(dt: datetime) -> Path:
    return DIR_5M / f"master_5m_{dt.strftime('%Y')}.csv"

# ─── Nifty 200 tickers (base symbols, .NS added automatically) ─────────────────
NIFTY200_TICKERS = [
    "RELIANCE", "TCS", "HDFCBANK", "BHARTIARTL", "ICICIBANK", "INFOSYS",
    "SBIN", "HINDUNILVR", "ITC", "LT", "KOTAKBANK", "AXISBANK", "BAJFINANCE",
    "ASIANPAINT", "MARUTI", "TITAN", "SUNPHARMA", "ULTRACEMCO", "WIPRO",
    "NESTLEIND", "ONGC", "POWERGRID", "NTPC", "COALINDIA", "JSWSTEEL",
    "TATAMOTORS", "TATASTEEL", "HCLTECH", "TECHM", "DIVISLAB", "DRREDDY",
    "CIPLA", "EICHERMOT", "HEROMOTOCO", "BAJAJFINSV", "ADANIENT", "ADANIPORTS",
    "APOLLOHOSP", "BAJAJ-AUTO", "BPCL", "BRITANNIA", "DABUR", "DLF",
    "GRASIM", "HAVELLS", "HINDALCO", "INDUSINDBK", "IOC", "M&M",
    "PIDILITIND", "SBILIFE", "SHREECEM", "SIEMENS", "TRENT", "VEDL",
    "ZOMATO", "DMART", "IRCTC", "BERGEPAINT", "BOSCHLTD", "CANBK",
    "CHOLAFIN", "COLPAL", "CONCOR", "CUMMINSIND", "ESCORTS", "FEDERALBNK",
    "GODREJCP", "GODREJPROP", "HAL", "HDFCLIFE", "HINDPETRO",
    "ICICIPRULI", "IDFCFIRSTB", "IGL", "INDUSTOWER",
    "JUBLFOOD", "LUPIN", "MARICO", "MPHASIS", "MUTHOOTFIN",
    "NAUKRI", "NBCC", "NHPC", "NMDC", "OFSS", "PAGEIND",
    "PERSISTENT", "PETRONET", "PFC", "PNB", "POLYCAB", "RBLBANK",
    "RECLTD", "SAIL", "SBICARD", "SHRIRAMFIN", "SRF",
    "SUNDARMFIN", "TATACHEM", "TATACONSUM", "TORNTPHARM", "TORNTPOWER",
    "TVSMOTOR", "UPL", "VOLTAS", "YESBANK", "ZYDUSLIFE",
    "ABCAPITAL", "ACC", "ADANIGREEN", "ALKEM", "AMBUJACEM",
    "ASTRAL", "AUROPHARMA", "BALKRISIND", "BANDHANBNK", "BANKBARODA",
    "BATAINDIA", "BEL", "BHARATFORG", "BHEL", "CDSL", "CESC",
    "DEEPAKNTR", "EMAMILTD", "EXIDEIND", "FORTIS", "GAIL", "GLENMARK",
    "GRANULES", "GUJGASLTD", "HINDCOPPER", "HUDCO", "IEX",
    "INDIGO", "IRFC", "JKCEMENT", "JSWENERGY",
    "KPITTECH", "LAURUSLABS", "LICHSGFIN", "LODHA", "LTTS",
    "MANAPPURAM", "MCX", "METROPOLIS", "MRPL", "NATIONALUM",
    "NAVINFLUOR", "OLECTRA", "PFIZER", "SUPREMEIND",
    "TATACOMM", "UBL", "UNITDSPR", "WHIRLPOOL", "CROMPTON",
]
INDEX_TICKER = "^NSEI"

# ─── Logging ───────────────────────────────────────────────────────────────────

def setup_logging():
    LOG_DIR.mkdir(exist_ok=True)
    log_file = LOG_DIR / f"pipeline_{datetime.now().strftime('%Y%m%d')}.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(),
        ],
    )

logger = logging.getLogger(__name__)

def ensure_dirs():
    for d in [DATA_DIR, LOG_DIR, DIR_1M, DIR_5M]:
        d.mkdir(parents=True, exist_ok=True)

# ─── Ticker helper ─────────────────────────────────────────────────────────────

def ticker_to_yf(ticker: str) -> str:
    if ticker.startswith("^") or ticker.endswith(".NS") or ticker.endswith(".BO"):
        return ticker
    return f"{ticker}.NS"

# ─── Fetch ─────────────────────────────────────────────────────────────────────

skipped_tickers = []

def fetch_ticker(symbol: str, interval: str) -> pd.DataFrame:
    yf_symbol  = ticker_to_yf(symbol)
    days_back  = DAYS_LIMIT[interval]
    end        = datetime.now()
    start      = end - timedelta(days=days_back)

    try:
        raw = yf.download(
            yf_symbol, start=start, end=end,
            interval=interval, progress=False,
            auto_adjust=True, multi_level_index=False,
        )
    except Exception as e:
        logger.warning(f"  SKIP {symbol} [{interval}] — {e}")
        skipped_tickers.append({"ticker": symbol, "interval": interval, "reason": str(e)})
        return pd.DataFrame(columns=COLUMNS)

    if raw is None or raw.empty:
        logger.warning(f"  SKIP {symbol} [{interval}] — no data (delisted?)")
        skipped_tickers.append({"ticker": symbol, "interval": interval, "reason": "no data"})
        return pd.DataFrame(columns=COLUMNS)

    if isinstance(raw.columns, pd.MultiIndex):
        raw.columns = raw.columns.get_level_values(0)

    raw = raw.reset_index()
    time_col = "Datetime" if "Datetime" in raw.columns else "Date"
    raw = raw.rename(columns={time_col: "datetime", "Open": "open", "High": "high",
                               "Low": "low", "Close": "close", "Volume": "volume"})

    missing = [c for c in ["datetime","open","high","low","close","volume"] if c not in raw.columns]
    if missing:
        logger.warning(f"  SKIP {symbol} [{interval}] — missing columns {missing}")
        skipped_tickers.append({"ticker": symbol, "interval": interval, "reason": f"missing {missing}"})
        return pd.DataFrame(columns=COLUMNS)

    raw["ticker"] = symbol
    raw = raw.dropna(subset=["open", "close"])

    if raw.empty:
        skipped_tickers.append({"ticker": symbol, "interval": interval, "reason": "all NaN"})
        return pd.DataFrame(columns=COLUMNS)

    raw["datetime"] = pd.to_datetime(raw["datetime"])
    if raw["datetime"].dt.tz is not None:
        raw["datetime"] = raw["datetime"].dt.tz_convert("Asia/Kolkata").dt.tz_localize(None)

    return raw[COLUMNS].reset_index(drop=True)


def fetch_all(tickers: list, interval: str) -> pd.DataFrame:
    frames = []
    total  = len(tickers)
    ok     = 0
    for i, sym in enumerate(tickers, 1):
        logger.info(f"  [{i:>3}/{total}] {sym:<20} {interval}")
        df = fetch_ticker(sym, interval)
        if not df.empty:
            frames.append(df)
            ok += 1
    logger.info(f"  → {ok}/{total} tickers OK for {interval}")
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=COLUMNS)

# ─── Split save/load for 1m (by month) ────────────────────────────────────────

def save_split_1m(df: pd.DataFrame):
    """
    Split 1m DataFrame by month and save each month to its own file.
    e.g. data/1m/master_1m_2026_03.csv
    """
    if df.empty:
        return
    df["_ym"] = df["datetime"].dt.to_period("M")
    for period, group in df.groupby("_ym"):
        path = DIR_1M / f"master_1m_{str(period).replace('-', '_')}.csv"
        group = group.drop(columns=["_ym"])
        _append_to_file(group, path)
    df.drop(columns=["_ym"], inplace=True)


def load_all_1m() -> pd.DataFrame:
    """Load and combine all monthly 1m files into one DataFrame."""
    files = sorted(DIR_1M.glob("master_1m_*.csv"))
    if not files:
        return pd.DataFrame(columns=COLUMNS)
    frames = [pd.read_csv(f, parse_dates=["datetime"]) for f in files]
    return pd.concat(frames, ignore_index=True)


# ─── Split save/load for 5m (by year) ─────────────────────────────────────────

def save_split_5m(df: pd.DataFrame):
    """
    Split 5m DataFrame by year and save each year to its own file.
    e.g. data/5m/master_5m_2026.csv
    """
    if df.empty:
        return
    df["_yr"] = df["datetime"].dt.year
    for year, group in df.groupby("_yr"):
        path = DIR_5M / f"master_5m_{year}.csv"
        group = group.drop(columns=["_yr"])
        _append_to_file(group, path)
    df.drop(columns=["_yr"], inplace=True)


def load_all_5m() -> pd.DataFrame:
    """Load and combine all yearly 5m files."""
    files = sorted(DIR_5M.glob("master_5m_*.csv"))
    if not files:
        return pd.DataFrame(columns=COLUMNS)
    frames = [pd.read_csv(f, parse_dates=["datetime"]) for f in files]
    return pd.concat(frames, ignore_index=True)


# ─── Generic file helpers ──────────────────────────────────────────────────────

def _append_to_file(new_df: pd.DataFrame, path: Path):
    """Append new_df to an existing CSV file, dedup, sort, save."""
    if path.exists() and path.stat().st_size > 0:
        try:
            existing = pd.read_csv(path, parse_dates=["datetime"])
            new_df   = pd.concat([existing, new_df], ignore_index=True)
        except Exception as e:
            logger.warning(f"  Could not read {path.name}: {e} — overwriting.")

    new_df = new_df.drop_duplicates(subset=["datetime", "ticker"])
    new_df = new_df.sort_values(["ticker", "datetime"]).reset_index(drop=True)
    new_df.to_csv(path, index=False)
    logger.info(f"    → {path.name}  ({len(new_df):,} rows)")


def load_master_1h() -> pd.DataFrame:
    if FILE_1H.exists() and FILE_1H.stat().st_size > 0:
        return pd.read_csv(FILE_1H, parse_dates=["datetime"])
    return pd.DataFrame(columns=COLUMNS)


def save_master_1h(new_df: pd.DataFrame):
    _append_to_file(new_df, FILE_1H)

# ─── Resample ──────────────────────────────────────────────────────────────────

def resample_ohlcv(df_1m: pd.DataFrame, rule: str) -> pd.DataFrame:
    if df_1m.empty:
        return pd.DataFrame(columns=COLUMNS)
    frames = []
    for ticker, grp in df_1m.groupby("ticker"):
        grp = grp.set_index("datetime").sort_index()
        try:
            r = grp[["open","high","low","close","volume"]].resample(rule).agg(
                {"open":"first","high":"max","low":"min","close":"last","volume":"sum"}
            ).dropna(subset=["open"])
            if r.empty:
                continue
            r["ticker"] = ticker
            frames.append(r.reset_index()[COLUMNS])
        except Exception as e:
            logger.warning(f"  Resample error {ticker}: {e}")
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=COLUMNS)

# ─── Backup ────────────────────────────────────────────────────────────────────

def backup_1h():
    if FILE_1H.exists():
        shutil.copy2(FILE_1H, BACKUP_1H)
        logger.info(f"  Backup → {BACKUP_1H.name}")

# ─── Validation ────────────────────────────────────────────────────────────────

def validate(generated: pd.DataFrame, actual: pd.DataFrame, timeframe: str) -> pd.DataFrame:
    issues = []
    if generated.empty or actual.empty:
        logger.warning(f"  Validation [{timeframe}] skipped — one dataset empty.")
        return pd.DataFrame()

    gen = generated.set_index(["ticker", "datetime"])
    act = actual.set_index(["ticker", "datetime"])

    for idx in act.index.difference(gen.index):
        issues.append({"timeframe": timeframe, "ticker": idx[0], "datetime": idx[1],
                       "issue": "missing_in_generated", "field": "", "generated": "", "actual": ""})
    for idx in gen.index.difference(act.index):
        issues.append({"timeframe": timeframe, "ticker": idx[0], "datetime": idx[1],
                       "issue": "missing_in_actual", "field": "", "generated": "", "actual": ""})

    common = gen.index.intersection(act.index)
    if not common.empty:
        gen_c, act_c = gen.loc[common], act.loc[common]
        for col in ["open", "high", "low", "close"]:
            diff = (gen_c[col] - act_c[col]).abs() / act_c[col].abs().clip(lower=1e-9)
            for idx in diff[diff > OHLC_TOLERANCE].index:
                issues.append({"timeframe": timeframe, "ticker": idx[0], "datetime": idx[1],
                               "issue": "ohlc_mismatch", "field": col,
                               "generated": round(float(gen_c.loc[idx, col]), 4),
                               "actual":    round(float(act_c.loc[idx, col]), 4)})
        vol_diff = (gen_c["volume"] - act_c["volume"]).abs() / act_c["volume"].abs().clip(lower=1)
        for idx in vol_diff[vol_diff > VOLUME_TOLERANCE].index:
            issues.append({"timeframe": timeframe, "ticker": idx[0], "datetime": idx[1],
                           "issue": "volume_mismatch", "field": "volume",
                           "generated": int(gen_c.loc[idx, "volume"]),
                           "actual":    int(act_c.loc[idx, "volume"])})

    n = len(issues)
    logger.warning(f"  Validation [{timeframe}]: {n} issues.") if n else logger.info(f"  Validation [{timeframe}]: ✓ Clean.")
    return pd.DataFrame(issues)


def save_validation_report(df: pd.DataFrame):
    if df.empty:
        logger.info("  No validation issues.")
        return
    if VALIDATION_REPORT.exists():
        df = pd.concat([pd.read_csv(VALIDATION_REPORT), df], ignore_index=True)
    df.to_csv(VALIDATION_REPORT, index=False)
    logger.info(f"  Validation report → {len(df)} rows")


def save_skipped_log():
    if not skipped_tickers:
        return
    df = pd.DataFrame(skipped_tickers)
    df["run_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if SKIPPED_LOG.exists():
        df = pd.concat([pd.read_csv(SKIPPED_LOG), df], ignore_index=True)
    df.to_csv(SKIPPED_LOG, index=False)
    logger.info(f"  Skipped: {len(skipped_tickers)} tickers → {SKIPPED_LOG.name}")

# ─── Cleanup old big master files (one-time migration) ────────────────────────

def cleanup_old_masters():
    """Delete old single-file masters if they exist (from v1/v2)."""
    for old in ["master_1m.csv", "master_5m.csv", "backup_master_1m.csv"]:
        p = DATA_DIR / old
        if p.exists():
            p.unlink()
            logger.info(f"  Removed old file: {old}")

# ─── Main ──────────────────────────────────────────────────────────────────────

def run():
    setup_logging()
    ensure_dirs()
    cleanup_old_masters()

    logger.info("=" * 60)
    logger.info("Indian Market Data Pipeline v3 — START")
    logger.info(f"Run time : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Tickers  : {len(NIFTY200_TICKERS)} stocks + Nifty Index")
    logger.info("File layout:")
    logger.info("  1m  → data/1m/master_1m_YYYY_MM.csv  (one per month)")
    logger.info("  5m  → data/5m/master_5m_YYYY.csv     (one per year)")
    logger.info("  1h  → data/master_1h.csv             (single file)")
    logger.info("=" * 60)

    all_tickers = NIFTY200_TICKERS + [INDEX_TICKER]

    # ── Step 1: Fetch & save 1m ───────────────────────────────────────────────
    logger.info("\n[1/5] Fetching 1-minute data...")
    df_1m = fetch_all(all_tickers, "1m")
    logger.info("  Saving 1m split by month:")
    save_split_1m(df_1m)

    # ── Step 2: Fetch & save 5m ───────────────────────────────────────────────
    logger.info("\n[2/5] Fetching 5-minute data...")
    df_5m_api = fetch_all(all_tickers, "5m")
    logger.info("  Saving 5m split by year:")
    save_split_5m(df_5m_api)

    # ── Step 3: Fetch & save 1h ───────────────────────────────────────────────
    logger.info("\n[3/5] Fetching 1-hour data...")
    df_1h_api = fetch_all(all_tickers, "1h")
    backup_1h()
    save_master_1h(df_1h_api)

    # ── Step 4: Resample 1m → 5m, 1h ─────────────────────────────────────────
    logger.info("\n[4/5] Resampling 1m → 5m and 1h...")
    full_1m   = load_all_1m()
    df_5m_gen = resample_ohlcv(full_1m, "5min")
    df_1h_gen = resample_ohlcv(full_1m, "1h")
    logger.info(f"  Generated 5m rows : {len(df_5m_gen):,}")
    logger.info(f"  Generated 1h rows : {len(df_1h_gen):,}")

    # ── Step 5: Validate ──────────────────────────────────────────────────────
    logger.info("\n[5/5] Validating...")
    full_5m_api = load_all_5m()
    full_1h_api = load_master_1h()
    issues = pd.concat([
        validate(df_5m_gen, full_5m_api, "5m"),
        validate(df_1h_gen, full_1h_api, "1h"),
    ], ignore_index=True)
    save_validation_report(issues)
    save_skipped_log()

    # ── Summary ───────────────────────────────────────────────────────────────
    files_1m = sorted(DIR_1M.glob("master_1m_*.csv"))
    files_5m = sorted(DIR_5M.glob("master_5m_*.csv"))
    logger.info("\n" + "=" * 60)
    logger.info("Pipeline COMPLETE ✓")
    logger.info(f"  1m files  : {len(files_1m)} monthly files in data/1m/")
    for f in files_1m:
        mb = f.stat().st_size / 1_048_576
        logger.info(f"    {f.name}  ({mb:.1f} MB)")
    logger.info(f"  5m files  : {len(files_5m)} yearly files in data/5m/")
    for f in files_5m:
        mb = f.stat().st_size / 1_048_576
        logger.info(f"    {f.name}  ({mb:.1f} MB)")
    if FILE_1H.exists():
        mb = FILE_1H.stat().st_size / 1_048_576
        logger.info(f"  1h file   : {FILE_1H.name}  ({mb:.1f} MB)")
    logger.info(f"  Skipped   : {len(skipped_tickers)} tickers")
    logger.info("=" * 60)


if __name__ == "__main__":
    run()
