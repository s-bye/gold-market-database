# Gold Market Intelligence — Database & AI Agent

A comprehensive analytical platform for gold market research. Combines a MySQL relational database, automated ETL pipelines, and an AI agent that lets users query financial data in natural language via Telegram.

---

## Features

- **Automated ETL** — data ingestion from Yahoo Finance and FRED (Federal Reserve Economic Data)
- **Structured schema** — 6 normalized tables linking asset prices, macro indicators, ETF flows, and historical market events
- **AI SQL Agent** — natural language interface (LangChain + Groq/Gemini) that translates questions into SQL queries
- **Telegram Bot** — on-demand analytics via chat + automated Morning Alerts with daily KPIs

---

## System Architecture

- collectdata.ipynb        ← downloads raw financial & macro data
- ↓
- transformdata.ipynb      ← cleans, resamples, calculates signals (MA, margins)
- ↓
- loadtomysql.ipynb        ← normalizes into 6 MySQL tables
- ↓
- telegrambot.py           ← AI Agent for natural language querying
- morningalert.py          ← daily KPI digest for subscribers

---

## Database Schema

6 core tables, indexed for analytical performance:

| Table | Rows | Description |
|---|---|---|
| `assets` | ~20 | Lookup table — symbols, names, categories (GC=F, SI=F, ^GSPC, etc.) |
| `dailyprices` | 6,000+ | OHLCV price history per asset (Yahoo Finance) |
| `macroindicators` | ~6,000 | Fed Rate, CPI, M2 Supply, Treasury yields, PPI (FRED, daily resampled) |
| `marketsignals` | ~6,000 | Derived signals — ETF signal, CFTC spec bull, VIX regime, Gold/Silver ratio |
| `marketevents` | 50–100 | Historical context — crises, Fed decisions, geopolitics, with gold impact label |
| `etfflows` | ~3,000 | GLD & IAU — price, 3-month return, premium to spot, total signal |

---

## Deployment

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Initialize the database

- Run the SQL scripts in your MySQL client in this order:

- ddloftables.sql    ← create table structure
- indexes.sql        ← optimize query performance
- views.sql          ← analytical views for problem statements
- procedures.sql     ← stored procedures for ROI and margin calculations

### 3. Run the ETL pipeline

- Execute notebooks in order:
- collectdata.ipynb     ← downloads fresh data, produces rawgolddata.csv
- transformdata.ipynb   ← processes raw data into normalized CSV tables
- loadtomysql.ipynb     ← uploads all prepared data to MySQL

### 4. Configure and launch

- Create a `.env` file in the root directory:

- GROQ_API_KEY=...        # or GEMINI_API_KEY
- TELEGRAM_BOT_TOKEN=...
- DB_HOST=...
- DB_USER=...
- DB_PASSWORD=...
- DB_NAME=...

- Start the bot:

```bash
python telegrambot.py
```

Schedule daily alerts (cron or task scheduler):

```bash
python morningalert.py
```

---

## Analytical Problem Statements

The system is pre-configured to answer:

1. **Portfolio optimization** — Under which macro conditions (Fed Rate, real rate, CPI) does gold produce the highest returns?
2. **Entry signals** — Which combination of signals (ETF inflow + CFTC bullish + low VIX) historically preceded a 5%+ gold surge?
3. **Crisis behavior** — How does gold perform relative to S&P 500 and oil during `marketevents` of type "crisis"?
4. **Mining profitability** — At what GoldPrice/MiningCostIndex ratio does extraction become unprofitable, and how does that affect price?
5. **Energy & margins** — How do oil prices and mining costs jointly affect gold producer margins during crises?

Example query to the AI agent:

> *"Show average gold growth in months when the Fed Rate was declining"*

The agent (LangChain + Gemini/Groq) generates SQL, executes it against MySQL, and returns the result with an interpretation.

---

## Tech Stack

- **Python** — Pandas, SQLAlchemy, LangChain, python-telegram-bot
- **Database** — MySQL
- **AI** — Google Gemini / Groq
- **Data sources** — Yahoo Finance (yfinance), FRED API

---

## Authors

- **Abay Nurlanov** — Ala-Too International University (AIU)
- **Bayastan Zamirbekov** — Ala-Too International University (AIU)
- **Bektur Momunov** — Ala-Too International University (AIU)
