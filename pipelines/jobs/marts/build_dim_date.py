from _db import connect, ensure_source_tables


def run_job() -> None:
    """Build DIM_DATE from the available event_ts window."""
    conn = connect()
    conn.autocommit = True
    try:
        ensure_source_tables(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS public.dim_date (
                    date_key INTEGER PRIMARY KEY,
                    calendar_date DATE NOT NULL UNIQUE,
                    year SMALLINT NOT NULL,
                    month SMALLINT NOT NULL,
                    day SMALLINT NOT NULL,
                    iso_week SMALLINT NOT NULL,
                    quarter SMALLINT NOT NULL,
                    is_weekend BOOLEAN NOT NULL
                );
                """
            )
            cur.execute(
                """
                WITH bounds AS (
                    SELECT
                        COALESCE(MIN(event_ts::date), CURRENT_DATE - INTERVAL '365 day')::date AS min_d,
                        COALESCE(MAX(event_ts::date), CURRENT_DATE + INTERVAL '30 day')::date AS max_d
                    FROM public.stock_bars_gold
                ),
                series AS (
                    SELECT generate_series(min_d, max_d, INTERVAL '1 day')::date AS d
                    FROM bounds
                )
                INSERT INTO public.dim_date (
                    date_key, calendar_date, year, month, day, iso_week, quarter, is_weekend
                )
                SELECT
                    TO_CHAR(d, 'YYYYMMDD')::int AS date_key,
                    d AS calendar_date,
                    EXTRACT(YEAR FROM d)::smallint,
                    EXTRACT(MONTH FROM d)::smallint,
                    EXTRACT(DAY FROM d)::smallint,
                    EXTRACT(WEEK FROM d)::smallint,
                    EXTRACT(QUARTER FROM d)::smallint,
                    EXTRACT(ISODOW FROM d) IN (6, 7) AS is_weekend
                FROM series
                ON CONFLICT (date_key) DO NOTHING;
                """
            )
        print("[OK] build_dim_date completed")
    finally:
        conn.close()


if __name__ == "__main__":
    run_job()
