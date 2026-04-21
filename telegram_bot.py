import os
from pathlib import Path
from dotenv import load_dotenv
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, filters, ContextTypes

from langchain_groq import ChatGroq
from langchain_community.utilities import SQLDatabase
from langchain_community.agent_toolkits import SQLDatabaseToolkit, create_sql_agent

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

Tables (exact column names):
assets: asset_id (PK), symbol, name, category, data_source
daily_prices: price_id (PK), asset_id (FK to assets.asset_id), price_date, close_price, open_price, high_price, low_price, volume
macro_indicators: macro_id (PK), indicator_date, fed_rate, treasury_10y, treasury_2y, real_rate_calc, yield_curve_10y2y, cpi_index, inflation_yoy, unemployment, m2_supply
etf_flows: asset_id (FK to assets.asset_id), flow_date, etf_price, gld_gold_premium, etf_signal
energy_mining: energy_id (PK), record_date, wti_oil, brent_oil, copper_price, mining_cost_index, gold_mining_margin, mining_bull_signal
market_events: event_id (PK), event_start, event_end, event_type, event_name, region, impact_on_gold

Asset symbols in assets table:
GC=F (Gold), SI=F (Silver), ^GSPC (S&P 500), DX-Y.NYB (DXY), ^VIX (VIX),
CL=F (WTI Oil), BZ=F (Brent Oil), HG=F (Copper), GDX (Gold Miners ETF),
GLD (SPDR Gold ETF), IAU (iShares Gold ETF), ^TNX (10Y Treasury),
^IRX (3M Treasury), TIPS (iShares TIPS ETF), RINF (Inflation ETF)

Ready-to-use views (prefer these over raw table joins):
v_gold_dxy_fed: price_date, gold_price, dxy_index, fed_rate, treasury_10y, inflation_yoy, real_rate_calc, yield_curve_10y2y
v_gold_during_events: event_name, event_type, region, impact_on_gold, event_start, event_end, price_date, gold_price, sp500_price
v_macro_predictors: indicator_date, fed_rate, treasury_10y, treasury_2y, real_rate_calc, yield_curve_10y2y, inflation_yoy, unemployment, m2_supply, gold_price, gold_return_1m, gold_up5pct_signal
v_asset_correlation: price_date, gold_price, silver_price, dxy_index, sp500_price, vix_index
v_energy_mining_crisis: record_date, wti_oil, brent_oil, copper_price, mining_cost_index, gold_mining_margin, mining_bull_signal, gold_price, event_name, event_type, impact_on_gold

Stored procedures:
CALL sp_gold_return_period('2020-01-01', '2020-12-31') - returns: period_start, period_end, gold_price_start, gold_price_end, absolute_return, return_pct
CALL sp_macro_regime_analysis(2.0) - returns: regime, trading_days, avg_gold_price, min_gold_price, max_gold_price, avg_inflation_yoy, avg_real_rate
CALL sp_mining_margin_history('2020-01-01', '2023-12-31') - returns: record_date, wti_oil, copper_price, mining_cost_index, gold_mining_margin, mining_bull_signal, gold_price, margin_30d_avg

Join rules (when writing raw SQL):
1. Gold = symbol 'GC=F' in assets
2. daily_prices - macro_indicators: price_date = indicator_date
3. daily_prices - energy_mining: price_date = record_date
4. daily_prices - market_events: price_date BETWEEN event_start AND COALESCE(event_end, CURDATE())

Output rules:
- Round prices to 2 decimal places, yields/rates to 4 decimal places
- Prefer VIEWs and stored procedures over writing raw JOINs from scratch
- If the question matches a stored procedure exactly - use CALL, not SELECT

STRICT RULES:
1. Schema is fully provided above — NEVER call sql_db_list_tables or sql_db_schema.
2. Call sql_db_query_checker EXACTLY ONCE per query, then immediately call sql_db_query.
3. Never repeat sql_db_query_checker on the same query.
4. To join daily_prices with assets always use: asset_id = (SELECT asset_id FROM assets WHERE symbol = 'GC=F')
"""

llm = ChatGroq(model="llama-3.1-8b-instant", temperature=0, api_key=GROQ_API_KEY)

class _QueryOnlyToolkit(SQLDatabaseToolkit):
    def get_tools(self):
        return [t for t in super().get_tools()
                if t.name in ("sql_db_query", "sql_db_query_checker")]

agent = create_sql_agent(
    llm=llm,
    toolkit=_QueryOnlyToolkit(db=db, llm=llm),
    agent_type="zero-shot-react-description",
    verbose=True,
    prefix=SYSTEM_PROMPT,
    max_iterations=5,
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
