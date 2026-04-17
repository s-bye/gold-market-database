-- daily_prices 
CREATE INDEX idx_dp_date     ON daily_prices(price_date);
CREATE INDEX idx_dp_asset    ON daily_prices(asset_id);
CREATE INDEX idx_dp_asset_date ON daily_prices(asset_id, price_date);

-- macro_indicators
CREATE INDEX idx_mi_date ON macro_indicators(indicator_date);

-- energy_mining
CREATE INDEX idx_em_date ON energy_mining(record_date);

-- market_events
CREATE INDEX idx_me_start ON market_events(event_start);
CREATE INDEX idx_me_end   ON market_events(event_end);