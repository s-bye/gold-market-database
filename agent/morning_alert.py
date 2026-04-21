import os
import sys
from datetime import date
from pathlib import Path

import pymysql
import requests
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / ".env")

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
DB_HOST        = os.getenv("DB_HOST")
DB_PORT        = int(os.getenv("DB_PORT", 3306))
DB_USER        = os.getenv("DB_USER")
DB_PASS        = os.getenv("DB_PASS")
DB_NAME        = os.getenv("DB_NAME")

GOLD_PRICE_SQL = """
    SELECT dp.price_date, dp.close_price
    FROM daily_prices dp
    JOIN assets a ON dp.asset_id = a.asset_id
    WHERE a.symbol = 'GC=F'
    ORDER BY dp.price_date DESC
    LIMIT 2
"""

MACRO_SQL = """
    SELECT fed_rate, real_rate_calc, inflation_yoy
    FROM macro_indicators
    ORDER BY indicator_date DESC
    LIMIT 1
"""

MINING_SQL = """
    SELECT gold_mining_margin, mining_bull_signal
    FROM energy_mining
    ORDER BY record_date DESC
    LIMIT 1
"""

EVENTS_SQL = """
    SELECT event_name
    FROM market_events
    WHERE event_end IS NULL OR event_end >= CURDATE()
    ORDER BY event_start DESC
"""


def fetch_all(cursor, sql):
    cursor.execute(sql)
    return cursor.fetchall()

def esc(value):
    return str(value).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def build_message(gold_rows, macro_row, mining_row, event_rows):
    today = date.today().strftime("%a, %b %d %Y")
    lines = [f"\U0001f305 <b>Gold Morning Brief</b> — {today}\n"]

    if gold_rows and len(gold_rows) >= 2:
        price_now  = float(gold_rows[0]["close_price"])
        price_prev = float(gold_rows[1]["close_price"])
        pct = (price_now - price_prev) / price_prev * 100
        arrow = "\U0001f4c8" if pct >= 0 else "\U0001f4c9"
        sign  = "+" if pct >= 0 else ""
        lines.append("\U0001f4b0 <b>Gold Price</b>")
        lines.append(f"${price_now:,.2f}  {arrow} {sign}{pct:.2f}%\n")
    else:
        lines.append("\U0001f4b0 <b>Gold Price</b>\nN/A\n")

    lines.append("\U0001f4ca <b>Macro</b>")
    if macro_row:
        fed  = macro_row["fed_rate"]
        real = macro_row["real_rate_calc"]
        inf  = macro_row["inflation_yoy"]
        lines.append(f"Fed Rate:   {float(fed):.2f}%" if fed is not None else "Fed Rate:   N/A")
        lines.append(f"Real Rate:  {float(real):.2f}%" if real is not None else "Real Rate:  N/A")
        lines.append(f"Inflation:  {float(inf):.2f}%\n" if inf is not None else "Inflation:  N/A\n")
    else:
        lines.append("N/A\n")

    lines.append("⛏ <b>Mining</b>")
    if mining_row:
        margin = mining_row["gold_mining_margin"]
        signal = mining_row["mining_bull_signal"]
        margin_str = f"${float(margin):,.2f}" if margin is not None else "N/A"
        signal_str = "\U0001f402 BULL" if signal else "\U0001f43b BEAR"
        lines.append(f"Mining Margin: {margin_str}")
        lines.append(f"Signal:        {signal_str}\n")
    else:
        lines.append("N/A\n")

    if event_rows:
        lines.append("⚠️ <b>Active Events</b>")
        for row in event_rows:
            lines.append(f"• {esc(row['event_name'])}")

    return "\n".join(lines)


SUBSCRIBERS_FILE = Path(__file__).parent / "subscribers.txt"


def load_subscribers():
    if not SUBSCRIBERS_FILE.exists():
        print("subscribers.txt not found - no one to send to.")
        return []
    ids = [line.strip() for line in SUBSCRIBERS_FILE.read_text().splitlines() if line.strip()]
    return ids


def send_telegram(text):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    for chat_id in load_subscribers():
        resp = requests.post(url, json={
            "chat_id":    chat_id,
            "text":       text,
            "parse_mode": "HTML",
        }, timeout=10)
        if not resp.ok:
            print(f"Error for chat_id {chat_id}:", resp.status_code, resp.text)
        else:
            print(f"Sent to {chat_id}")


def main():
    conn = pymysql.connect(
        host=DB_HOST, port=DB_PORT, user=DB_USER,
        password=DB_PASS, database=DB_NAME,
        cursorclass=pymysql.cursors.DictCursor,
    )
    try:
        with conn.cursor() as cur:
            gold_rows  = fetch_all(cur, GOLD_PRICE_SQL)
            macro_row  = fetch_all(cur, MACRO_SQL)
            mining_row = fetch_all(cur, MINING_SQL)
            event_rows = fetch_all(cur, EVENTS_SQL)
    finally:
        conn.close()

    macro_row  = macro_row[0]  if macro_row  else None
    mining_row = mining_row[0] if mining_row else None

    message = build_message(gold_rows, macro_row, mining_row, event_rows)
    send_telegram(message)
    print("Alert sent.")


if __name__ == "__main__":
    main()
