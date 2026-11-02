import os

from pyflink.table import EnvironmentSettings, TableEnvironment

KAFKA = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "agri.sensor.readings")
PG_HOST = os.getenv("POSTGRES_HOST", "postgres")
PG_DB = os.getenv("POSTGRES_DB", "agristream")
PG_USER = os.getenv("POSTGRES_USER", "agri")
PG_PASS = os.getenv("POSTGRES_PASSWORD", "agri_secret")


def main():
    t_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())

    cfg = t_env.get_config()
    cfg.set("pipeline.name", "agristream-1m-field-aggregation")
    cfg.set("execution.checkpointing.interval", "30 s")
    cfg.set("table.exec.source.idle-timeout", "10 s")
    cfg.set("restart-strategy.type", "fixed-delay")
    cfg.set("restart-strategy.fixed-delay.attempts", "10")
    cfg.set("restart-strategy.fixed-delay.delay", "10 s")

    t_env.execute_sql(f"""
        CREATE TABLE sensor_readings (
            sensor_id     STRING,
            field_id      STRING,
            crop          STRING,
            soil_moisture DOUBLE,
            temperature_c DOUBLE,
            humidity      DOUBLE,
            ph            DOUBLE,
            event_time    TIMESTAMP(3),
            WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{TOPIC}',
            'properties.bootstrap.servers' = '{KAFKA}',
            'properties.group.id' = 'agristream-flink',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json',
            'json.timestamp-format.standard' = 'ISO-8601',
            'json.ignore-parse-errors' = 'true'
        )
    """)

    t_env.execute_sql(f"""
        CREATE TABLE field_metrics_1m (
            window_start      TIMESTAMP(3),
            window_end        TIMESTAMP(3),
            field_id          STRING,
            crop              STRING,
            reading_count     BIGINT,
            avg_soil_moisture DOUBLE,
            avg_temperature_c DOUBLE,
            max_temperature_c DOUBLE,
            avg_humidity      DOUBLE,
            avg_ph            DOUBLE,
            PRIMARY KEY (window_start, field_id) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://{PG_HOST}:5432/{PG_DB}',
            'table-name' = 'field_metrics_1m',
            'username' = '{PG_USER}',
            'password' = '{PG_PASS}',
            'sink.buffer-flush.max-rows' = '100',
            'sink.buffer-flush.interval' = '2 s',
            'sink.max-retries' = '3'
        )
    """)

    t_env.execute_sql("""
        INSERT INTO field_metrics_1m
        SELECT
            window_start,
            window_end,
            field_id,
            crop,
            COUNT(*)           AS reading_count,
            AVG(soil_moisture) AS avg_soil_moisture,
            AVG(temperature_c) AS avg_temperature_c,
            MAX(temperature_c) AS max_temperature_c,
            AVG(humidity)      AS avg_humidity,
            AVG(ph)            AS avg_ph
        FROM TABLE(
            TUMBLE(TABLE sensor_readings, DESCRIPTOR(event_time), INTERVAL '1' MINUTE)
        )
        GROUP BY window_start, window_end, field_id, crop
    """)


if __name__ == "__main__":
    main()