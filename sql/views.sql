USE gold_market;


-- VIEWS (5) — one per problem statement

-- P1: How do DXY and real Fed rate affect gold price?

DROP VIEW IF EXISTS v_gold_dxy_fed;
CREATE VIEW v_gold_dxy_fed AS
SELECT
    dp.price_date,
    dp.close_price                          AS gold_price,
    dxy.close_price                         AS dxy_index,
    mi.fed_rate,
    mi.treasury_10y,
    mi.inflation_yoy,
    mi.real_rate_calc,
    mi.yield_curve_10y2y
FROM daily_prices dp
JOIN assets a_gold
    ON dp.asset_id = a_gold.asset_id AND a_gold.symbol = 'GC=F'
LEFT JOIN assets a_dxy
    ON a_dxy.symbol = 'DX-Y.NYB'
LEFT JOIN daily_prices dxy
    ON dxy.price_date = dp.price_date AND dxy.asset_id = a_dxy.asset_id
LEFT JOIN macro_indicators mi
    ON mi.indicator_date = dp.price_date
ORDER BY dp.price_date;



-- P2: How does gold behave during geopolitical events vs S&P 500?


DROP VIEW IF EXISTS v_gold_during_events;
CREATE VIEW v_gold_during_events AS
SELECT
    me.event_name,
    me.event_type,
    me.region,
    me.impact_on_gold,
    me.event_start,
    me.event_end,
    dp.price_date,
    dp.close_price                          AS gold_price,
    sp.close_price                          AS sp500_price
FROM market_events me
JOIN assets a_gold
    ON a_gold.symbol = 'GC=F'
JOIN daily_prices dp
    ON dp.asset_id = a_gold.asset_id
    AND dp.price_date BETWEEN me.event_start AND COALESCE(me.event_end, CURDATE())
LEFT JOIN assets a_sp
    ON a_sp.symbol = '^GSPC'
LEFT JOIN daily_prices sp
    ON sp.asset_id = a_sp.asset_id AND sp.price_date = dp.price_date
ORDER BY me.event_name, dp.price_date;



-- P3: Which macro factors best predict gold growth > 5% per month?

DROP VIEW IF EXISTS v_macro_predictors;
CREATE VIEW v_macro_predictors AS
SELECT
    mi.indicator_date,
    mi.fed_rate,
    mi.treasury_10y,
    mi.treasury_2y,
    mi.real_rate_calc,
    mi.yield_curve_10y2y,
    mi.inflation_yoy,
    mi.unemployment,
    mi.m2_supply,
    dp.close_price                          AS gold_price,
    ROUND(
        (dp.close_price /
            LAG(dp.close_price, 21) OVER (ORDER BY dp.price_date) - 1) * 100,
        4
    )                                       AS gold_return_1m,
    CASE
        WHEN (dp.close_price /
            LAG(dp.close_price, 21) OVER (ORDER BY dp.price_date) - 1) * 100 > 5
        THEN 1 ELSE 0
    END                                     AS gold_up5pct_signal
FROM macro_indicators mi
JOIN assets a_gold
    ON a_gold.symbol = 'GC=F'
JOIN daily_prices dp
    ON dp.asset_id = a_gold.asset_id AND dp.price_date = mi.indicator_date
ORDER BY mi.indicator_date;



-- P4: How do gold, oil, silver, and dollar correlate across periods?

DROP VIEW IF EXISTS v_asset_correlation;
CREATE VIEW v_asset_correlation AS
SELECT
    dp_gold.price_date,
    dp_gold.close_price                     AS gold_price,
    dp_si.close_price                       AS silver_price,
    dp_dxy.close_price                      AS dxy_index,
    dp_sp.close_price                       AS sp500_price,
    dp_vix.close_price                      AS vix_index
FROM daily_prices dp_gold
JOIN assets a_gold
    ON dp_gold.asset_id = a_gold.asset_id AND a_gold.symbol = 'GC=F'
LEFT JOIN assets a_si  ON a_si.symbol  = 'SI=F'
LEFT JOIN assets a_dxy ON a_dxy.symbol = 'DX-Y.NYB'
LEFT JOIN assets a_sp  ON a_sp.symbol  = '^GSPC'
LEFT JOIN assets a_vix ON a_vix.symbol = '^VIX'
LEFT JOIN daily_prices dp_si
    ON dp_si.asset_id = a_si.asset_id   AND dp_si.price_date  = dp_gold.price_date
LEFT JOIN daily_prices dp_dxy
    ON dp_dxy.asset_id = a_dxy.asset_id AND dp_dxy.price_date = dp_gold.price_date
LEFT JOIN daily_prices dp_sp
    ON dp_sp.asset_id = a_sp.asset_id   AND dp_sp.price_date  = dp_gold.price_date
LEFT JOIN daily_prices dp_vix
    ON dp_vix.asset_id = a_vix.asset_id AND dp_vix.price_date = dp_gold.price_date
ORDER BY dp_gold.price_date;



-- P5: How do oil + mining costs affect gold margin during crises?

DROP VIEW IF EXISTS v_energy_mining_crisis;
CREATE VIEW v_energy_mining_crisis AS
SELECT
    em.record_date,
    em.wti_oil,
    em.brent_oil,
    em.copper_price,
    em.mining_cost_index,
    em.gold_mining_margin,
    em.mining_bull_signal,
    dp.close_price                          AS gold_price,
    me.event_name,
    me.event_type,
    me.impact_on_gold
FROM energy_mining em
JOIN assets a_gold
    ON a_gold.symbol = 'GC=F'
JOIN daily_prices dp
    ON dp.asset_id = a_gold.asset_id AND dp.price_date = em.record_date
LEFT JOIN market_events me
    ON em.record_date BETWEEN me.event_start AND COALESCE(me.event_end, CURDATE())
ORDER BY em.record_date;