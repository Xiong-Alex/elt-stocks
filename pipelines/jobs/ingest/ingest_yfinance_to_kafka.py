import argparse


def run_job(mode: str) -> None:
    # TODO: Pull symbols from config/DB, fetch yfinance OHLCV, normalize payload,
    # and publish messages to KAFKA_TOPIC with symbol as key.
    print(f"[STUB] ingest_yfinance_to_kafka mode={mode}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", default="intraday")
    args = parser.parse_args()
    run_job(args.mode)
