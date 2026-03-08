import argparse


def run_job(mode: str) -> None:
    # TODO: Create Spark readStream from Kafka, parse/validate schema, route invalid
    # records to quarantine, and write valid rows to Bronze with checkpointing.
    print(f"[STUB] spark_kafka_to_bronze mode={mode}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", default="intraday")
    args = parser.parse_args()
    run_job(args.mode)
