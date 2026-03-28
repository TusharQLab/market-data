"""
Indian Market Data Pipeline v2
Fixes:
  - Ticker format: always uses .NS suffix (RELIANCE.NS not RELIANCE)
  - API date limits respected (1m=7d, 5m=59d, 1h=720d)
  - Empty DataFrame handled safely everywhere
  - Delisted/bad tickers skipped cleanly with logging
  - yfinance MultiIndex columns handled (new yfinance behavior)
  - Timezone stripped properly
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

DATA_DIR = Path("data")
LOG_DIR  = Path("logs")

FILES = {
    "1m": DATA_DIR / "master_1m.csv",
    "5m": DATA_DIR / "master_5m.csv",
    "1h": DATA_DIR / "master_1h.csv",
}
BACKUP_1M        = DATA_DIR / "backup_master_1m.csv"
VALIDATION_REPORT = DATA_DIR / "validation_report.csv"
SKIPPED_LOG      = DATA_DIR / "skipped_tickers.csv"

# Tolerances for validation
OHLC_TOLERANCE   = 0.01   # 1%
VOLUME_TOLERANCE = 0.05   # 5%

COLUMNS = ["datetime", "ticker", "open", "high", "low", "close", "volume"]

# yfinance hard limits (use slightly less to be safe)
DAYS_LIMIT = {"1m": 7, "5m": 59, "1h": 720}

# ─── Nifty 200 — clean, verified Yahoo Finance symbols (WITHOUT .NS) ───────────
# .NS is added automatically by ticker_to_yf()
NIFTY200_TICKERS = [
    # Large Cap
    "RELIANCE", "TCS", "HDFCBANK", "BHARTIARTL", "ICICIBANK", "INFOSYS",
    "SBIN", "HINDUNILVR", "ITC", "LT", "KOTAKBANK", "AXISBANK", "BAJFINANCE",
    "ASIANPAINT", "MARUTI", "TITAN", "SUNPHARMA", "ULTRACEMCO", "WIPRO",
    "NESTLEIND", "ONGC", "POWERGRID", "NTPC", "COALINDIA", "JSWSTEEL",
    "TATAMOTORS", "TATASTEEL", "HCLTECH", "TECHM", "DIVISLAB", "DRREDDY",
    "CIPLA", "EICHERMOT", "HEROMOTOCO", "BAJAJFINSV", "ADANIENT", "ADANIPORTS",
    "APOLLOHOSP", "BAJAJ-AUTO", "BPCL", "BRITANNIA", "DABUR", "DLF",
    "GRASIM", "HAVELLS", "HINDALCO", "INDUSINDBK", "IOC", "M&M",
    # Mid Cap
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
    "NAVINFLUOR", "OLECTRA", "PFIZER", "POLYCAB", "SUPREMEIND",
    "TATACOMM", "UBL", "UNITDSPR", "WHIRLPOOL", "CROMPTON",
]

INDEX_TICKER = "^NSEI"

# ─── Logging setup ─────────────────────────────────────────────────────────────

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

# ─── Directory setup ───────────────────────────────────────────────────────────

def ensure_dirs():
    DATA_DIR.mkdir(exist_ok=True)
    LOG_DIR.mkdir(exist_ok=True)

# ─── Ticker format ─────────────────────────────────────────────────────────────

def ticker_to_yf(ticker: str) -> str:
    """
    Convert base ticker to Yahoo Finance format.
    RELIANCE  → RELIANCE.NS
    ^NSEI     → ^NSEI  (index, no suffix)
    Already has .NS → unchanged
    """
    if ticker.startswith("^"):
        return ticker
    if ticker.endswith(".NS") or ticker.endswith(".BO"):
        return ticker
    return f"{ticker}.NS"

# ─── Safe data fetch ───────────────────────────────────────────────────────────

skipped_tickers = []   # global list, saved at the end

def fetch_ticker(symbol: str, interval: str) -> pd.DataFrame:
    """
    Fetch OHLCV for one symbol from Yahoo Finance.
    Returns clean DataFrame or empty DataFrame on any failure.
    Handles:
      - yfinance MultiIndex columns (new behavior)
      - Timezone stripping
      - Empty / delisted tickers
      - API date limits
    """
    yf_symbol = ticker_to_yf(symbol)
    days_back  = DAYS_LIMIT[interval]
    end   = datetime.now()
    start = end - timedelta(days=days_back)

    try:
        raw = yf.download(
            yf_symbol,
            start=start,
            end=end,
            interval=interval,
            progress=False,
            auto_adjust=True,
            multi_level_index=False,   # forces flat columns in newer yfinance
        )
    except Exception as e:
        logger.warning(f"  SKIP {symbol} [{interval}] — download error: {e}")
        skipped_tickers.append({"ticker": symbol, "interval": interval, "reason": str(e)})
        return pd.DataFrame(columns=COLUMNS)

    # ── Guard: empty result ───────────────────────────────────────────────────
    if raw is None or raw.empty:
        logger.warning(f"  SKIP {symbol} [{interval}] — no data returned (possibly delisted)")
        skipped_tickers.append({"ticker": symbol, "interval": interval, "reason": "no data / delisted"})
        return pd.DataFrame(columns=COLUMNS)

    # ── Flatten MultiIndex columns if present ─────────────────────────────────
    if isinstance(raw.columns, pd.MultiIndex):
        raw.columns = raw.columns.get_level_values(0)

    raw = raw.reset_index()

    # ── Rename columns (yfinance uses 'Datetime' for intraday, 'Date' for daily)
    time_col = "Datetime" if "Datetime" in raw.columns else "Date"
    rename_map = {
        time_col: "datetime",
        "Open":   "open",
        "High":   "high",
        "Low":    "low",
        "Close":  "close",
        "Volume": "volume",
    }
    raw = raw.rename(columns=rename_map)

    # ── Guard: required columns must exist ───────────────────────────────────
    missing_cols = [c for c in COLUMNS if c not in raw.columns and c != "ticker"]
    if missing_cols:
        logger.warning(f"  SKIP {symbol} [{interval}] — missing columns: {missing_cols}")
        skipped_tickers.append({"ticker": symbol, "interval": interval, "reason": f"missing cols {missing_cols}"})
        return pd.DataFrame(columns=COLUMNS)

    raw["ticker"] = symbol

    # ── Drop rows where open or close is NaN ─────────────────────────────────
    raw = raw.dropna(subset=["open", "close"])

    if raw.empty:
        logger.warning(f"  SKIP {symbol} [{interval}] — all rows NaN after cleaning")
        skipped_tickers.append({"ticker": symbol, "interval": interval, "reason": "all NaN"})
        return pd.DataFrame(columns=COLUMNS)

    # ── Strip timezone so all datetimes are naive (no tz offset) ─────────────
    raw["datetime"] = pd.to_datetime(raw["datetime"])
    if raw["datetime"].dt.tz is not None:
        raw["datetime"] = raw["datetime"].dt.tz_convert("Asia/Kolkata").dt.tz_localize(None)

    # ── Select and return clean columns ──────────────────────────────────────
    return raw[COLUMNS].reset_index(drop=True)


def fetch_all(tickers: list, interval: str) -> pd.DataFrame:
    """Fetch all tickers for a given interval. Skips failures gracefully."""
    frames = []
    total  = len(tickers)
    ok     = 0

    for i, sym in enumerate(tickers, 1):
        logger.info(f"  [{i:>3}/{total}] {sym:<20} {interval}")
        df = fetch_ticker(sym, interval)
        if not df.empty:
            frames.append(df)
            ok += 1

    logger.info(f"  → Fetched {ok}/{total} tickers successfully for {interval}")
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=COLUMNS)

# ─── Resample ──────────────────────────────────────────────────────────────────

def resample_ohlcv(df_1m: pd.DataFrame, rule: str) -> pd.DataFrame:
    """
    Resample 1-minute data to a higher timeframe.
    rule: '5min' for 5-minute, '1h' for hourly
    """
    if df_1m.empty:
        logger.warning("Resample skipped — 1m data is empty.")
        return pd.DataFrame(columns=COLUMNS)

    frames = []
    for ticker, grp in df_1m.groupby("ticker"):
        grp = grp.set_index("datetime").sort_index()
        try:
            resampled = grp[["open", "high", "low", "close", "volume"]].resample(rule).agg({
                "open":   "first",
                "high":   "max",
                "low":    "min",
                "close":  "last",
                "volume": "sum",
            }).dropna(subset=["open"])

            if resampled.empty:
                continue

            resampled["ticker"] = ticker
            resampled = resampled.reset_index()
            frames.append(resampled[COLUMNS])
        except Exception as e:
            logger.warning(f"  Resample error for {ticker}: {e}")
            continue

    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=COLUMNS)

# ─── Master file helpers ───────────────────────────────────────────────────────

def load_master(path: Path) -> pd.DataFrame:
    if path.exists() and path.stat().st_size > 0:
        try:
            df = pd.read_csv(path, parse_dates=["datetime"])
            return df
        except Exception as e:
            logger.warning(f"Could not load {path}: {e}")
    return pd.DataFrame(columns=COLUMNS)


def save_master(df: pd.DataFrame, path: Path):
    if df.empty:
        logger.warning(f"  Nothing to save for {path.name} — skipping.")
        return
    df = df.drop_duplicates(subset=["datetime", "ticker"])
    df = df.sort_values(["ticker", "datetime"]).reset_index(drop=True)
    df.to_csv(path, index=False)
    logger.info(f"  Saved {len(df):,} rows → {path.name}")


def append_and_save(new_df: pd.DataFrame, path: Path):
    """Merge new data into existing master file."""
    existing = load_master(path)
    combined = pd.concat([existing, new_df], ignore_index=True)
    save_master(combined, path)

# ─── Backup ────────────────────────────────────────────────────────────────────

def backup_1m():
    if FILES["1m"].exists():
        shutil.copy2(FILES["1m"], BACKUP_1M)
        logger.info(f"  Backup saved → {BACKUP_1M.name}")

# ─── Validation ────────────────────────────────────────────────────────────────

def validate(generated: pd.DataFrame, actual: pd.DataFrame, timeframe: str) -> pd.DataFrame:
    """
    Compare generated candles (resampled from 1m) vs actual API candles.
    Returns DataFrame of issues found.
    """
    issues = []

    if generated.empty or actual.empty:
        logger.warning(f"  Validation [{timeframe}] skipped — one dataset is empty.")
        return pd.DataFrame()

    gen = generated.set_index(["ticker", "datetime"])
    act = actual.set_index(["ticker", "datetime"])

    # 1. Missing timestamps
    for idx in act.index.difference(gen.index):
        issues.append({"timeframe": timeframe, "ticker": idx[0], "datetime": idx[1],
                       "issue": "missing_in_generated", "field": "", "generated": "", "actual": ""})

    for idx in gen.index.difference(act.index):
        issues.append({"timeframe": timeframe, "ticker": idx[0], "datetime": idx[1],
                       "issue": "missing_in_actual", "field": "", "generated": "", "actual": ""})

    # 2. OHLC mismatch on common rows
    common      = gen.index.intersection(act.index)
    if common.empty:
        return pd.DataFrame(issues)

    gen_c = gen.loc[common]
    act_c = act.loc[common]

    for col in ["open", "high", "low", "close"]:
        rel_diff = (gen_c[col] - act_c[col]).abs() / act_c[col].abs().clip(lower=1e-9)
        for idx in rel_diff[rel_diff > OHLC_TOLERANCE].index:
            issues.append({"timeframe": timeframe, "ticker": idx[0], "datetime": idx[1],
                           "issue": "ohlc_mismatch", "field": col,
                           "generated": round(float(gen_c.loc[idx, col]), 4),
                           "actual":    round(float(act_c.loc[idx, col]), 4)})

    # 3. Volume mismatch
    vol_diff = (gen_c["volume"] - act_c["volume"]).abs() / act_c["volume"].abs().clip(lower=1)
    for idx in vol_diff[vol_diff > VOLUME_TOLERANCE].index:
        issues.append({"timeframe": timeframe, "ticker": idx[0], "datetime": idx[1],
                       "issue": "volume_mismatch", "field": "volume",
                       "generated": int(gen_c.loc[idx, "volume"]),
                       "actual":    int(act_c.loc[idx, "volume"])})

    count = len(issues)
    if count:
        logger.warning(f"  Validation [{timeframe}]: {count} issues found.")
    else:
        logger.info(f"  Validation [{timeframe}]: ✓ All clean.")

    return pd.DataFrame(issues)


def save_validation_report(report_df: pd.DataFrame):
    if report_df.empty:
        logger.info("  No validation issues to save.")
        return
    if VALIDATION_REPORT.exists():
        existing = pd.read_csv(VALIDATION_REPORT)
        report_df = pd.concat([existing, report_df], ignore_index=True)
    report_df.to_csv(VALIDATION_REPORT, index=False)
    logger.info(f"  Validation report → {VALIDATION_REPORT.name} ({len(report_df)} total rows)")


def save_skipped_log():
    if not skipped_tickers:
        return
    df = pd.DataFrame(skipped_tickers)
    df["run_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if SKIPPED_LOG.exists():
        old = pd.read_csv(SKIPPED_LOG)
        df = pd.concat([old, df], ignore_index=True)
    df.to_csv(SKIPPED_LOG, index=False)
    logger.info(f"  Skipped tickers log → {SKIPPED_LOG.name} ({len(skipped_tickers)} this run)")

# ─── Main ──────────────────────────────────────────────────────────────────────

def run():
    setup_logging()
    ensure_dirs()

    logger.info("=" * 60)
    logger.info("Indian Market Data Pipeline v2 — START")
    logger.info(f"Run time : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Tickers  : {len(NIFTY200_TICKERS)} stocks + Nifty Index")
    logger.info("=" * 60)

    all_tickers = NIFTY200_TICKERS + [INDEX_TICKER]

    # ── Step 1: Fetch 1m ──────────────────────────────────────────────────────
    logger.info("\n[1/5] Fetching 1-minute data (last 7 days)...")
    df_1m = fetch_all(all_tickers, "1m")
    backup_1m()
    append_and_save(df_1m, FILES["1m"])

    # ── Step 2: Fetch 5m ──────────────────────────────────────────────────────
    logger.info("\n[2/5] Fetching 5-minute data (last 59 days)...")
    df_5m_api = fetch_all(all_tickers, "5m")
    append_and_save(df_5m_api, FILES["5m"])

    # ── Step 3: Fetch 1h ──────────────────────────────────────────────────────
    logger.info("\n[3/5] Fetching 1-hour data (last 720 days)...")
    df_1h_api = fetch_all(all_tickers, "1h")
    append_and_save(df_1h_api, FILES["1h"])

    # ── Step 4: Resample ──────────────────────────────────────────────────────
    logger.info("\n[4/5] Resampling 1m → 5m and 1h...")
    full_1m   = load_master(FILES["1m"])
    df_5m_gen = resample_ohlcv(full_1m, "5min")
    df_1h_gen = resample_ohlcv(full_1m, "1h")
    logger.info(f"  Generated 5m rows : {len(df_5m_gen):,}")
    logger.info(f"  Generated 1h rows : {len(df_1h_gen):,}")

    # ── Step 5: Validate ──────────────────────────────────────────────────────
    logger.info("\n[5/5] Validating generated vs API candles...")
    report_5m  = validate(df_5m_gen, load_master(FILES["5m"]), "5m")
    report_1h  = validate(df_1h_gen, load_master(FILES["1h"]), "1h")
    all_issues = pd.concat([report_5m, report_1h], ignore_index=True)
    save_validation_report(all_issues)

    # ── Save skipped tickers ──────────────────────────────────────────────────
    save_skipped_log()

    # ── Summary ───────────────────────────────────────────────────────────────
    logger.info("\n" + "=" * 60)
    logger.info("Pipeline COMPLETE ✓")
    logger.info(f"  master_1m rows   : {len(load_master(FILES['1m'])):,}")
    logger.info(f"  master_5m rows   : {len(load_master(FILES['5m'])):,}")
    logger.info(f"  master_1h rows   : {len(load_master(FILES['1h'])):,}")
    logger.info(f"  Tickers skipped  : {len(skipped_tickers)}")
    logger.info("=" * 60)


if __name__ == "__main__":
    run()
