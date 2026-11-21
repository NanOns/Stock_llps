ALTER TABLE stock.source_t0_early_observation_facts
    DROP CONSTRAINT source_t0_early_price_coordinate_ck;

ALTER TABLE stock.source_t0_early_observation_facts
    ADD CONSTRAINT source_t0_early_price_coordinate_ck
    CHECK (price_coordinate = 'FORWARD_ADJUSTED');

COMMENT ON COLUMN stock.source_t0_early_observation_facts.price_coordinate IS
    '价格坐标；固定FORWARD_ADJUSTED供应方前复权，板内RPS5/RPS20名次及PULSE相对解释使用该坐标；属于NON_PIT后验研究。';
