-- STORED PROCEDURES (3)



-- SP1: Gold return for any date range

DROP PROCEDURE IF EXISTS sp_gold_return_period;
DELIMITER $$
CREATE PROCEDURE sp_gold_return_period(
    IN p_start DATE,
    IN p_end   DATE
)
BEGIN
    DECLARE v_gold_id     INT;
    DECLARE v_price_start DECIMAL(15,4);
    DECLARE v_price_end   DECIMAL(15,4);
 
    SELECT asset_id INTO v_gold_id FROM assets WHERE symbol = 'GC=F' LIMIT 1;
 
    SELECT close_price INTO v_price_start
    FROM daily_prices
    WHERE asset_id = v_gold_id AND price_date >= p_start
    ORDER BY price_date ASC LIMIT 1;
 
    SELECT close_price INTO v_price_end
    FROM daily_prices
    WHERE asset_id = v_gold_id AND price_date <= p_end
    ORDER BY price_date DESC LIMIT 1;
 
    SELECT
        p_start                                             AS period_start,
        p_end                                               AS period_end,
        v_price_start                                       AS gold_price_start,
        v_price_end                                         AS gold_price_end,
        ROUND(v_price_end - v_price_start, 4)               AS absolute_return,
        ROUND((v_price_end / v_price_start - 1) * 100, 4)  AS return_pct;
END$$
DELIMITER ;



-- SP2: Average gold return by Fed rate regime

DROP PROCEDURE IF EXISTS sp_macro_regime_analysis;
DELIMITER $$
CREATE PROCEDURE sp_macro_regime_analysis(
    IN p_fed_threshold DECIMAL(6,4)
)
BEGIN
    DECLARE v_gold_id INT;
    SELECT asset_id INTO v_gold_id FROM assets WHERE symbol = 'GC=F' LIMIT 1;
 
    SELECT
        CASE
            WHEN mi.fed_rate < p_fed_threshold THEN CONCAT('Fed < ', p_fed_threshold, '%')
            ELSE CONCAT('Fed >= ', p_fed_threshold, '%')
        END                                             AS regime,
        COUNT(*)                                        AS trading_days,
        ROUND(AVG(dp.close_price), 2)                  AS avg_gold_price,
        ROUND(MIN(dp.close_price), 2)                  AS min_gold_price,
        ROUND(MAX(dp.close_price), 2)                  AS max_gold_price,
        ROUND(AVG(mi.inflation_yoy), 4)                AS avg_inflation_yoy,
        ROUND(AVG(mi.real_rate_calc), 4)               AS avg_real_rate
    FROM macro_indicators mi
    JOIN daily_prices dp
        ON dp.price_date = mi.indicator_date AND dp.asset_id = v_gold_id
    WHERE mi.fed_rate IS NOT NULL
    GROUP BY regime
    ORDER BY regime;
END$$
DELIMITER ;



-- SP3: Mining margin history for any date range

DROP PROCEDURE IF EXISTS sp_mining_margin_history;
DELIMITER $$
CREATE PROCEDURE sp_mining_margin_history(
    IN p_start DATE,
    IN p_end   DATE
)
BEGIN
    DECLARE v_gold_id INT;
    SELECT asset_id INTO v_gold_id FROM assets WHERE symbol = 'GC=F' LIMIT 1;
 
    SELECT
        em.record_date,
        em.wti_oil,
        em.copper_price,
        em.mining_cost_index,
        em.gold_mining_margin,
        em.mining_bull_signal,
        dp.close_price                                  AS gold_price,
        ROUND(AVG(em.gold_mining_margin)
            OVER (ORDER BY em.record_date
                  ROWS BETWEEN 29 PRECEDING AND CURRENT ROW), 4)
                                                        AS margin_30d_avg
    FROM energy_mining em
    JOIN daily_prices dp
        ON dp.price_date = em.record_date AND dp.asset_id = v_gold_id
    WHERE em.record_date BETWEEN p_start AND p_end
    ORDER BY em.record_date;
END$$
DELIMITER ;