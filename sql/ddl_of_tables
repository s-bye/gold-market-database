USE gold_market;

CREATE TABLE IF NOT EXISTS assets (
    asset_id    INT PRIMARY KEY AUTO_INCREMENT,
    symbol      VARCHAR(20)  NOT NULL UNIQUE,
    name        VARCHAR(100),
    category    VARCHAR(50),
    data_source VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS daily_prices (
    price_id    INT PRIMARY KEY AUTO_INCREMENT,
    asset_id    INT NOT NULL,
    price_date  DATE NOT NULL,
    close_price DECIMAL(15,4),
    open_price  DECIMAL(15,4),
    high_price  DECIMAL(15,4),
    low_price   DECIMAL(15,4),
    volume      BIGINT,
    FOREIGN KEY (asset_id) REFERENCES assets(asset_id)
);

CREATE TABLE IF NOT EXISTS macro_indicators (
    macro_id        INT PRIMARY KEY AUTO_INCREMENT,
    indicator_date  DATE NOT NULL,
    fed_rate        DECIMAL(6,4),
    treasury_10y    DECIMAL(6,4),
    treasury_2y     DECIMAL(6,4),
    real_rate_calc  DECIMAL(8,4),
    yield_curve_10y2y DECIMAL(8,4),
    cpi_index       DECIMAL(10,4),
    inflation_yoy   DECIMAL(8,4),
    unemployment    DECIMAL(6,4),
    m2_supply       DECIMAL(20,4)
);

CREATE TABLE IF NOT EXISTS market_events (
    event_id        INT PRIMARY KEY AUTO_INCREMENT,
    event_start     DATE NOT NULL,
    event_end       DATE,
    event_type      VARCHAR(50),
    event_name      VARCHAR(200),
    region          VARCHAR(100),
    impact_on_gold  VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS etf_flows (
    flow_id          INT PRIMARY KEY AUTO_INCREMENT,
    asset_id         INT NOT NULL,
    flow_date        DATE NOT NULL,
    etf_price        DECIMAL(15,4),
    etf_volume       BIGINT,
    gld_gold_premium DECIMAL(8,4),
    etf_signal       TINYINT,
    FOREIGN KEY (asset_id) REFERENCES assets(asset_id)
);

CREATE TABLE IF NOT EXISTS energy_mining (
    energy_id           INT PRIMARY KEY AUTO_INCREMENT,
    record_date         DATE NOT NULL,
    wti_oil             DECIMAL(10,4),
    brent_oil           DECIMAL(10,4),
    copper_price        DECIMAL(10,4),
    mining_cost_index   DECIMAL(10,4),
    gold_mining_margin  DECIMAL(10,4),
    mining_bull_signal  TINYINT
);