import os
from pathlib import Path
from dotenv import load_dotenv
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, filters, ContextTypes

from langchain_groq import ChatGroq
from langchain_community.utilities import SQLDatabase
from langchain_community.agent_toolkits import create_sql_agent

load_dotenv()

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
GROQ_API_KEY   = os.getenv("GROQ_API_KEY")
DB_HOST        = os.getenv("DB_HOST")
DB_PORT        = os.getenv("DB_PORT")
DB_USER        = os.getenv("DB_USER")
DB_PASS        = os.getenv("DB_PASS")
DB_NAME        = os.getenv("DB_NAME")

db = SQLDatabase.from_uri(
    f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}",
    include_tables=[
        "assets", "daily_prices", "macro_indicators",
        "etf_flows", "energy_mining", "market_events",
    ],
    sample_rows_in_table_info=2,
)

SYSTEM_PROMPT = """
You are a gold market analyst. You have access to the gold_market MySQL database.

Tables:
assets: 15 financial assets reference (symbol, name, category, data_source)
daily_prices: daily close prices since 2000 (asset_id FK: assets, price_date, close_price)
macro_indicators: Fed Rate, treasury_10y, treasury_2y, real_rate_calc, yield_curve_10y2y, cpi_index, inflation_yoy, unemployment, m2_supply
etf_flows: GLD and IAU ETFs (asset_id FK: assets, flow_date, etf_price, gld_gold_premium, etf_signal)
energy_mining: wti_oil, brent_oil, copper_price, mining_cost_index, gold_mining_margin, mining_bull_signal
market_events: event_name, event_type, region, impact_on_gold, event_start, event_end

Ready-to-use views (prefer these over raw table joins):
v_gold_dxy_fed - gold price + DXY + Fed Rate + real_rate_calc + inflation by date
v_gold_during_events - gold price + sp500 for each day inside a market event period
v_macro_predictors - gold price + all macro + gold_return_1m + gold_up5pct_signal
v_asset_correlation - gold, silver, DXY, S&P500, VIX side by side by date
v_energy_mining_crisis - energy/mining data + gold price + event info by date

Stored procedures:
CALL sp_gold_return_period('2020-01-01', '2020-12-31') - return % for any date range
CALL sp_macro_regime_analysis(2.0) - avg gold price above/below fed threshold
CALL sp_mining_margin_history('2020-01-01', '2023-12-31') - margin history with 30d moving avg

Join rules (when writing raw SQL):
1. Gold = symbol 'GC=F' in assets
2. daily_prices - macro_indicators: price_date = indicator_date
3. daily_prices - energy_mining: price_date = record_date
4. daily_prices - market_events: price_date BETWEEN event_start AND COALESCE(event_end, CURDATE())

Output rules:
- Round prices to 2 decimal places, yields/rates to 4 decimal places
- Prefer VIEWs and stored procedures over writing raw JOINs from scratch
- If the question matches a stored procedure exactly - use CALL, not SELECT
"""

llm = ChatGroq(model="llama-3.1-8b-instant", temperature=0, api_key=GROQ_API_KEY)

agent = create_sql_agent(
    llm=llm,
    db=db,
    agent_type="zero-shot-react-description",
    verbose=True,
    prefix=SYSTEM_PROMPT,
    max_iterations=10,
    handle_parsing_errors=True,
)

SUBSCRIBERS_FILE = Path(__file__).parent / "subscribers.txt"

def save_subscriber(chat_id: int):
    ids = set()
    if SUBSCRIBERS_FILE.exists():
        ids = set(SUBSCRIBERS_FILE.read_text().split())
    if str(chat_id) not in ids:
        with SUBSCRIBERS_FILE.open("a") as f:
            f.write(f"{chat_id}\n")

async def handle_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    save_subscriber(update.message.chat_id)
    await update.message.reply_text(
        "👋 Hello! I'm a gold market analyst bot.\n"
        "Ask me anything about gold prices, macro indicators, or market events."
    )

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    save_subscriber(update.message.chat_id)
    question = update.message.text
    await update.message.reply_text("Thinking...")

    result = agent.invoke({"input": question})
    await update.message.reply_text(result["output"])

app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()
app.add_handler(CommandHandler("start", handle_start))
app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

print("Started")
app.run_polling(drop_pending_updates=True)
