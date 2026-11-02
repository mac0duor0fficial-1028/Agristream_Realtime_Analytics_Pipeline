CREATE TABLE IF NOT EXISTS field_metrics_1m (
    window_start       TIMESTAMP(3)     NOT NULL,
    window_end         TIMESTAMP(3)     NOT NULL,
    field_id           VARCHAR(64)      NOT NULL,
    crop               VARCHAR(64),
    reading_count      BIGINT,
    avg_soil_moisture  DOUBLE PRECISION,
    avg_temperature_c  DOUBLE PRECISION,
    max_temperature_c  DOUBLE PRECISION,
    avg_humidity       DOUBLE PRECISION,
    avg_ph             DOUBLE PRECISION,
    PRIMARY KEY (window_start, field_id)
);

CREATE INDEX IF NOT EXISTS idx_field_metrics_field_time
    ON field_metrics_1m (field_id, window_start DESC);

CREATE OR REPLACE VIEW latest_field_status AS
SELECT DISTINCT ON (field_id) *
FROM field_metrics_1m
ORDER BY field_id, window_start DESC;

CREATE OR REPLACE VIEW irrigation_alerts AS
SELECT field_id, crop, window_start, avg_soil_moisture, max_temperature_c,
       CASE
         WHEN avg_soil_moisture < 25 THEN 'LOW_SOIL_MOISTURE'
         WHEN max_temperature_c > 35 THEN 'HEAT_STRESS'
       END AS alert_type
FROM latest_field_status
WHERE avg_soil_moisture < 25 OR max_temperature_c > 35;