def run_job() -> None:
    # TODO: Fetch fundamentals per symbol, upsert raw table, and optionally publish
    # to KAFKA_TOPIC_FUNDAMENTALS for downstream streaming consumers.
    print("[STUB] ingest_yfinance_fundamentals")


if __name__ == "__main__":
    run_job()
