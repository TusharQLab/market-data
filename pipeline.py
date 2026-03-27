"""
Indian Market Data Pipeline
Fetches 1m/5m/1h data for Nifty 200 + ^NSEI, validates, stores, and backs up.
"""

import os
import logging
import warnings
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import yfinance as yf

warnings.filterwarnings("ignore")

# ─── Config ────────────────────────────────────────────────────────────────────

DATA_DIR = Path("data")
LOG_DIR = Path("logs")

FILES = {
    "1m": DATA_DIR / "master_1m.csv",
    "5m": DATA_DIR / "master_5m.csv",
    "1h": DATA_DIR / "master_1h.csv",
}
BACKUP_1M = DATA_DIR / "backup_master_1m.csv"
VALIDATION_REPORT = DATA_DIR / "validation_report.csv"

OHLC_TOLERANCE = 0.01   # 1% relative tolerance
VOLUME_TOLERANCE = 0.05  # 5% relative tolerance

COLUMNS = ["datetime", "ticker", "open", "high", "low", "close", "volume"]

# ─── Nifty 200 tickers (Yahoo Finance format) ──────────────────────────────────
# Full list — appended with .NS for NSE
NIFTY200_TICKERS = [
    "RELIANCE", "TCS", "HDFCBANK", "BHARTIARTL", "ICICIBANK", "INFOSYS",
    "SBIN", "HINDUNILVR", "ITC", "LT", "KOTAKBANK", "AXISBANK", "BAJFINANCE",
    "ASIANPAINT", "MARUTI", "TITAN", "SUNPHARMA", "ULTRACEMCO", "WIPRO",
    "NESTLEIND", "ONGC", "POWERGRID", "NTPC", "COALINDIA", "JSWSTEEL",
    "TATAMOTORS", "TATASTEEL", "HCLTECH", "TECHM", "DIVISLAB", "DRREDDY",
    "CIPLA", "EICHERMOT", "HEROMOTOCO", "BAJAJFINSV", "ADANIENT", "ADANIPORTS",
    "APOLLOHOSP", "BAJAJ-AUTO", "BPCL", "BRITANNIA", "DABUR", "DLF",
    "GRASIM", "HAVELLS", "HINDALCO", "INDUSINDBK", "IOC", "M&M",
    "MCDOWELL-N", "MOTHERSON", "PIDILITIND", "SBILIFE", "SHREECEM",
    "SIEMENS", "TRENT", "VEDL", "ZOMATO", "PAYTM", "NYKAA",
    "DMART", "IRCTC", "BERGEPAINT", "BIOCON", "BOSCHLTD", "CANBK",
    "CHOLAFIN", "COLPAL", "CONCOR", "CUMMINSIND", "ESCORTS", "FEDERALBNK",
    "GMRINFRA", "GODREJCP", "GODREJPROP", "HAL", "HDFCLIFE", "HINDPETRO",
    "ICICIPRULI", "IDEA", "IDFCFIRSTB", "IGL", "INDUSTOWER", "INFRATEL",
    "JUBLFOOD", "L&TFH", "LICI", "LUPIN", "MARICO", "MFSL",
    "MPHASIS", "MUTHOOTFIN", "NAUKRI", "NBCC", "NHPC", "NMDC",
    "OBEROIRLTY", "OFSS", "PAGEIND", "PEL", "PERSISTENT", "PETRONET",
    "PFC", "PNB", "POLYCAB", "RBLBANK", "RECLTD", "SAIL",
    "SBICARD", "SHRIRAMFIN", "SRF", "STAR", "SUNDARMFIN", "SUPREMEIND",
    "TATACHEM", "TATACOMM", "TATACONSUM", "TORNTPHARM", "TORNTPOWER",
    "TVSMOTOR", "UBL", "UNITDSPR", "UPL", "VOLTAS", "WHIRLPOOL",
    "YESBANK", "ZEEL", "ZYDUSLIFE", "ABCAPITAL", "ABFRL", "ACC",
    "ADANIGREEN", "ADANITRANS", "ALKEM", "AMBUJACEM", "APLAPOLLO",
    "ASTRAL", "AUROPHARMA", "BALKRISIND", "BANDHANBNK", "BANKBARODA",
    "BATAINDIA", "BEL", "BHARATFORG", "BHEL", "CANFINHOME", "CASTROLIND",
    "CDSL", "CESC", "CG", "CHAMBLFERT", "CROMPTON", "CRISIL",
    "DEEPAKNTR", "DELTACORP", "EDELWEISS", "EMAMILTD", "ENGINERSIN",
    "EQUITAS", "EXIDEIND", "FORTIS", "FSL", "GAIL", "GLENMARK",
    "GNFC", "GPPL", "GRANULES", "GSPL", "GUJGASLTD", "HINDCOPPER",
    "HUDCO", "IDBI", "IEX", "IFCI", "IIFL", "INDIANB",
    "INDIGO", "INOXWIND", "IOB", "IRCON", "IRFC", "ISEC",
    "JKCEMENT", "JSL", "JSWENERGY", "JUBILANT", "KAJARIACER", "KALPATPOWR",
    "KPITTECH", "KRBL", "LATENTVIEW", "LAURUSLABS", "LICHSGFIN", "LINDE",
    "LODHA", "LTTS", "MANAPPURAM", "MASFIN", "MCX", "METROPOLIS",
    "MINDTREE", "MOTILALOFS", "MRPL", "NATIONALUM", "NAVINFLUOR", "NFL",
    "NIITMTS", "NOCIL", "OLECTRA", "ORIENTELEC", "PCJEWELLER", "PFIZER",
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

# ─── Helpers ───────────────────────────────────────────────────────────────────

def ensure_dirs():
    DATA_DIR.mkdir(exist_ok=True)
    LOG_DIR.mkdir(exist_ok=True)


def load_master(path: Path) -> pd.DataFrame:
    if path.exists():
        df = pd.read_csv(path, parse_dates=["datetime"])
        return df
    return pd.DataFrame(columns=COLUMNS)


def save_master(df: pd.DataFrame, path: Path):
    df = df.drop_duplicates(subset=["datetime", "ticker"])
    df = df.sort_values(["ticker", "datetime"]).reset_index(drop=True)
    df.to_csv(path, index=False)
    logger.info(f"Saved {len(df):,} rows → {path}")


def ticker_to_yf(ticker: str) -> str:
    """Add .NS suffix for NSE stocks."""
    return ticker if ticker.startswith("^") else f"{ticker}.NS"


# ─── Fetch ─────────────────────────────────────────────────────────────────────

def fetch_ticker(symbol: str, interval: str, days_back: int) -> pd.DataFrame:
    """
    Fetch OHLCV from Yahoo Finance.
    Returns a clean DataFrame with COLUMNS format.
    """
    yf_symbol = ticker_to_yf(symbol)
    end = datetime.now()
    start = end - timedelta(days=days_back)

    try:
        raw = yf.download(
            yf_symbol,
            start=start,
            end=end,
            interval=interval,
            progress=False,
            auto_adjust=True,
        )
        if raw.empty:
            logger.warning(f"No data: {symbol} [{interval}]")
            return pd.DataFrame(columns=COLUMNS)

        raw = raw.reset_index()
        # yfinance column name varies: Datetime vs Date
        time_col = "Datetime" if "Datetime" in raw.columns else "Date"
        raw = raw.rename(columns={
            time_col: "datetime",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume",
        })
        raw["ticker"] = symbol
        raw = raw[COLUMNS].dropna(subset=["open", "close"])
        raw["datetime"] = pd.to_datetime(raw["datetime"]).dt.tz_localize(None)
        return raw

    except Exception as e:
        logger.error(f"Fetch error {symbol} [{interval}]: {e}")
        return pd.DataFrame(columns=COLUMNS)


def fetch_all(tickers: list[str], interval: str, days_back: int) -> pd.DataFrame:
    frames = []
    total = len(tickers)
    for i, sym in enumerate(tickers, 1):
        logger.info(f"  [{i}/{total}] {sym} {interval}")
        df = fetch_ticker(sym, interval, days_back)
        if not df.empty:
            frames.append(df)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=COLUMNS)


# ─── Resample ──────────────────────────────────────────────────────────────────

def resample_ohlcv(df_1m: pd.DataFrame, rule: str) -> pd.DataFrame:
    """Resample 1m data to a higher timeframe ('5T' or '1h')."""
    if df_1m.empty:
        return pd.DataFrame(columns=COLUMNS)

    frames = []
    for ticker, grp in df_1m.groupby("ticker"):
        grp = grp.set_index("datetime").sort_index()
        resampled = grp[["open", "high", "low", "close", "volume"]].resample(rule).agg({
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last",
            "volume": "sum",
        }).dropna(subset=["open"])
        resampled["ticker"] = ticker
        resampled = resampled.reset_index().rename(columns={"index": "datetime"})
        frames.append(resampled[COLUMNS])

    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=COLUMNS)


# ─── Append ────────────────────────────────────────────────────────────────────

def append_and_save(new_df: pd.DataFrame, path: Path):
    existing = load_master(path)
    combined = pd.concat([existing, new_df], ignore_index=True)
    save_master(combined, path)


# ─── Backup ────────────────────────────────────────────────────────────────────

def backup_1m():
    if FILES["1m"].exists():
        import shutil
        shutil.copy2(FILES["1m"], BACKUP_1M)
        logger.info(f"Backup saved → {BACKUP_1M}")


# ─── Validation ────────────────────────────────────────────────────────────────

def validate(generated: pd.DataFrame, actual: pd.DataFrame, timeframe: str) -> pd.DataFrame:
    """
    Compare generated (from 1m resample) vs actual (from API).
    Returns a DataFrame of issues.
    """
    issues = []

    if generated.empty or actual.empty:
        logger.warning(f"Validation skipped for {timeframe}: one dataset is empty.")
        return pd.DataFrame()

    gen = generated.set_index(["ticker", "datetime"])
    act = actual.set_index(["ticker", "datetime"])

    # 1. Missing timestamps
    missing_in_gen = act.index.difference(gen.index)
    for idx in missing_in_gen:
        issues.append({
            "timeframe": timeframe,
            "ticker": idx[0],
            "datetime": idx[1],
            "issue": "missing_in_generated",
            "field": "",
            "generated": "",
            "actual": "",
        })

    missing_in_act = gen.index.difference(act.index)
    for idx in missing_in_act:
        issues.append({
            "timeframe": timeframe,
            "ticker": idx[0],
            "datetime": idx[1],
            "issue": "missing_in_actual",
            "field": "",
            "generated": "",
            "actual": "",
        })

    # 2. OHLCV differences on common rows
    common = gen.index.intersection(act.index)
    gen_common = gen.loc[common]
    act_common = act.loc[common]

    for col in ["open", "high", "low", "close"]:
        tol = OHLC_TOLERANCE
        diff = (gen_common[col] - act_common[col]).abs() / act_common[col].abs().clip(lower=1e-9)
        bad = diff[diff > tol]
        for idx in bad.index:
            issues.append({
                "timeframe": timeframe,
                "ticker": idx[0],
                "datetime": idx[1],
                "issue": f"ohlc_mismatch",
                "field": col,
                "generated": round(gen_common.loc[idx, col], 4),
                "actual": round(act_common.loc[idx, col], 4),
            })

    # 3. Volume mismatch
    vol_diff = (gen_common["volume"] - act_common["volume"]).abs() / act_common["volume"].abs().clip(lower=1)
    vol_bad = vol_diff[vol_diff > VOLUME_TOLERANCE]
    for idx in vol_bad.index:
        issues.append({
            "timeframe": timeframe,
            "ticker": idx[0],
            "datetime": idx[1],
            "issue": "volume_mismatch",
            "field": "volume",
            "generated": int(gen_common.loc[idx, "volume"]),
            "actual": int(act_common.loc[idx, "volume"]),
        })

    if issues:
        logger.warning(f"Validation [{timeframe}]: {len(issues)} issues found.")
    else:
        logger.info(f"Validation [{timeframe}]: ✓ All clean.")

    return pd.DataFrame(issues)


def save_validation_report(report_df: pd.DataFrame):
    if report_df.empty:
        return
    if VALIDATION_REPORT.exists():
        existing = pd.read_csv(VALIDATION_REPORT)
        report_df = pd.concat([existing, report_df], ignore_index=True)
    report_df.to_csv(VALIDATION_REPORT, index=False)
    logger.info(f"Validation report saved → {VALIDATION_REPORT} ({len(report_df)} rows)")


# ─── Main Pipeline ─────────────────────────────────────────────────────────────

def run():
    setup_logging()
    ensure_dirs()
    logger.info("=" * 60)
    logger.info("Indian Market Data Pipeline — START")
    logger.info(f"Run time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)

    all_tickers = NIFTY200_TICKERS + [INDEX_TICKER]

    # yfinance 1m limit = 7 days; 5m/1h = 60 days
    DAYS = {"1m": 7, "5m": 60, "1h": 365}

    # ── 1. Fetch 1m data ──────────────────────────────────────────────────────
    logger.info("\n[1/5] Fetching 1-minute data...")
    df_1m_new = fetch_all(all_tickers, "1m", DAYS["1m"])
    backup_1m()
    append_and_save(df_1m_new, FILES["1m"])

    # ── 2. Fetch 5m and 1h from API ───────────────────────────────────────────
    logger.info("\n[2/5] Fetching 5-minute data (API)...")
    df_5m_api = fetch_all(all_tickers, "5m", DAYS["5m"])
    append_and_save(df_5m_api, FILES["5m"])

    logger.info("\n[3/5] Fetching 1-hour data (API)...")
    df_1h_api = fetch_all(all_tickers, "1h", DAYS["1h"])
    append_and_save(df_1h_api, FILES["1h"])

    # ── 3. Resample 1m → 5m, 1h ──────────────────────────────────────────────
    logger.info("\n[4/5] Resampling 1m → 5m and 1h...")
    full_1m = load_master(FILES["1m"])
    df_5m_gen = resample_ohlcv(full_1m, "5min")
    df_1h_gen = resample_ohlcv(full_1m, "1h")
    logger.info(f"  Generated 5m rows: {len(df_5m_gen):,}")
    logger.info(f"  Generated 1h rows: {len(df_1h_gen):,}")

    # ── 4. Validate ───────────────────────────────────────────────────────────
    logger.info("\n[5/5] Validating generated vs API data...")
    full_5m_api = load_master(FILES["5m"])
    full_1h_api = load_master(FILES["1h"])

    report_5m = validate(df_5m_gen, full_5m_api, "5m")
    report_1h = validate(df_1h_gen, full_1h_api, "1h")
    all_issues = pd.concat([report_5m, report_1h], ignore_index=True)
    save_validation_report(all_issues)

    # ── Done ──────────────────────────────────────────────────────────────────
    logger.info("\n" + "=" * 60)
    logger.info("Pipeline COMPLETE ✓")
    logger.info(f"  master_1m rows  : {len(load_master(FILES['1m'])):,}")
    logger.info(f"  master_5m rows  : {len(load_master(FILES['5m'])):,}")
    logger.info(f"  master_1h rows  : {len(load_master(FILES['1h'])):,}")
    logger.info("=" * 60)


if __name__ == "__main__":
    run()
