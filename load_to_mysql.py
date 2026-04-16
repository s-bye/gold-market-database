"""
load_to_mysql.py
================
Шаг 3 ETL: Читаем подготовленные CSV и загружаем в MySQL.
Порядок важен из-за FK: сначала assets, потом все остальные таблицы.
"""

import pandas as pd
from sqlalchemy import create_engine, text

# ── Подключение к MySQL ───────────────────────────────────────────────────────

DB_HOST = "204.168.228.132"
DB_PORT = 3306
DB_NAME = "gold_market"
DB_USER = "gold_user"
DB_PASS = "gold_pass_2024"

ENGINE = create_engine(
    f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}",
    echo=False
)

# ── Справочник активов (assets) ───────────────────────────────────────────────
#
# Заполняется вручную один раз. 15 строк.
# category: precious_metal / equity_index / currency / etf / commodity

ASSETS = [
    # symbol,       name,                          category,        data_source
    ("GC=F",        "Gold Futures",                "precious_metal", "yfinance"),
    ("SI=F",        "Silver Futures",              "precious_metal", "yfinance"),
    ("PL=F",        "Platinum Futures",            "precious_metal", "yfinance"),
    ("PA=F",        "Palladium Futures",           "precious_metal", "yfinance"),
    ("DX-Y.NYB",    "US Dollar Index (DXY)",       "currency",       "yfinance"),
    ("^GSPC",       "S&P 500 Index",               "equity_index",   "yfinance"),
    ("^VIX",        "CBOE Volatility Index",       "equity_index",   "yfinance"),
    ("EURUSD=X",    "EUR/USD Exchange Rate",       "currency",       "yfinance"),
    ("USDCNY=X",    "USD/CNY Exchange Rate",       "currency",       "yfinance"),
    ("RUB=X",       "USD/RUB Exchange Rate",       "currency",       "yfinance"),
    ("GLD",         "SPDR Gold ETF",               "etf",            "yfinance"),
    ("IAU",         "iShares Gold ETF",            "etf",            "yfinance"),
    ("CL=F",        "WTI Crude Oil Futures",       "commodity",      "yfinance"),
    ("BZ=F",        "Brent Crude Oil Futures",     "commodity",      "yfinance"),
    ("HG=F",        "Copper Futures",              "commodity",      "yfinance"),
]

# ─────────────────────────────────────────────────────────────────────────────

def load_assets(conn):
    """Загружает справочник активов. Пропускает дубликаты."""
    print("  Загружаем assets...")
    df = pd.DataFrame(ASSETS, columns=["symbol", "name", "category", "data_source"])

    # INSERT IGNORE — если актив уже есть, пропускаем
    for _, row in df.iterrows():
        conn.execute(text("""
            INSERT IGNORE INTO assets (symbol, name, category, data_source)
            VALUES (:symbol, :name, :category, :data_source)
        """), row.to_dict())

    count = conn.execute(text("SELECT COUNT(*) FROM assets")).scalar()
    print(f"    assets: {count} записей")


def get_asset_id_map(conn) -> dict:
    """Возвращает словарь symbol → asset_id."""
    result = conn.execute(text("SELECT symbol, asset_id FROM assets"))
    return {row[0]: row[1] for row in result}


def load_daily_prices(conn, asset_id_map: dict):
    """
    Загружает ежедневные цены.
    CSV содержит колонку symbol — заменяем на asset_id через справочник.
    """
    print("  Загружаем daily_prices...")
    df = pd.read_csv("table_daily_prices.csv", parse_dates=["price_date"])

    df["asset_id"] = df["symbol"].map(asset_id_map)
    df = df.dropna(subset=["asset_id"])
    df["asset_id"] = df["asset_id"].astype(int)
    df = df[["asset_id", "price_date", "close_price"]]

    df.to_sql("daily_prices", conn, if_exists="append", index=False, method="multi", chunksize=1000)
    count = conn.execute(text("SELECT COUNT(*) FROM daily_prices")).scalar()
    print(f"    daily_prices: {count} записей")


def load_macro_indicators(conn):
    """Загружает макроэкономические данные."""
    print("  Загружаем macro_indicators...")
    df = pd.read_csv("table_macro_indicators.csv", parse_dates=["indicator_date"])

    df.to_sql("macro_indicators", conn, if_exists="append", index=False, method="multi", chunksize=500)
    count = conn.execute(text("SELECT COUNT(*) FROM macro_indicators")).scalar()
    print(f"    macro_indicators: {count} записей")


def load_etf_flows(conn, asset_id_map: dict):
    """Загружает данные ETF фондов."""
    print("  Загружаем etf_flows...")
    df = pd.read_csv("table_etf_flows.csv", parse_dates=["flow_date"])

    df["asset_id"] = df["symbol"].map(asset_id_map)
    df = df.dropna(subset=["asset_id"])
    df["asset_id"] = df["asset_id"].astype(int)
    df = df[["asset_id", "flow_date", "etf_price", "gld_gold_premium", "etf_signal"]]

    df.to_sql("etf_flows", conn, if_exists="append", index=False, method="multi", chunksize=500)
    count = conn.execute(text("SELECT COUNT(*) FROM etf_flows")).scalar()
    print(f"    etf_flows: {count} записей")


def load_energy_mining(conn):
    """Загружает данные энергетики и себестоимости добычи."""
    print("  Загружаем energy_mining...")
    df = pd.read_csv("table_energy_mining.csv", parse_dates=["record_date"])

    df.to_sql("energy_mining", conn, if_exists="append", index=False, method="multi", chunksize=500)
    count = conn.execute(text("SELECT COUNT(*) FROM energy_mining")).scalar()
    print(f"    energy_mining: {count} записей")


def load_to_mysql():
    print("=== load_to_mysql.py: загрузка в MySQL ===\n")

    with ENGINE.begin() as conn:
        # Порядок важен: assets первым (FK зависимость)
        load_assets(conn)
        asset_id_map = get_asset_id_map(conn)
        print(f"  asset_id_map: {len(asset_id_map)} активов\n")

        load_daily_prices(conn, asset_id_map)
        load_macro_indicators(conn)
        load_etf_flows(conn, asset_id_map)
        load_energy_mining(conn)

    print("\n=== Готово — все таблицы загружены ===")
    print("  Таблица market_events заполняется вручную через SQL (исторические события).")


if __name__ == "__main__":
    load_to_mysql()
