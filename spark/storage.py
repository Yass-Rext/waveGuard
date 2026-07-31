from pyspark.sql import DataFrame

from config import (
    NORMAL_PATH,
    FRAUD_PATH,
    AUDIT_PATH,
    CHECKPOINT_DIR,
)


def write_normal_transactions(df: DataFrame):

    return (
        df.writeStream
        .format("parquet")
        .option("path", NORMAL_PATH)
        .option(
            "checkpointLocation",
            f"{CHECKPOINT_DIR}/normal"
        )
        .outputMode("append")
        .start()
    )


def write_fraud_transactions(df: DataFrame):

    return (
        df.writeStream
        .format("parquet")
        .option("path", FRAUD_PATH)
        .option(
            "checkpointLocation",
            f"{CHECKPOINT_DIR}/fraud"
        )
        .outputMode("append")
        .start()
    )


def write_audit_logs(df: DataFrame):

    return (
        df.writeStream
        .format("parquet")
        .option("path", AUDIT_PATH)
        .option(
            "checkpointLocation",
            f"{CHECKPOINT_DIR}/audit"
        )
        .outputMode("append")
        .start()
    )