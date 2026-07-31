from pyspark.sql.functions import to_json
from pyspark.sql.functions import struct

from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    FRAUD_TOPIC,
    AUDIT_TOPIC,
    CHECKPOINT_DIR
)


def write_fraud_topic(df):

    kafka_df = (
        df.select(
            to_json(
                struct("*")
            ).alias("value")
        )
    )

    return (
        kafka_df
        .writeStream
        .format("kafka")
        .option(
            "kafka.bootstrap.servers",
            KAFKA_BOOTSTRAP_SERVERS
        )
        .option(
            "topic",
            FRAUD_TOPIC
        )
        .option(
            "checkpointLocation",
            f"{CHECKPOINT_DIR}/fraud_topic"
        )
        .outputMode("append")
        .start()
    )


def write_audit_topic(df):

    kafka_df = (
        df.select(
            to_json(
                struct("*")
            ).alias("value")
        )
    )

    return (
        kafka_df
        .writeStream
        .format("kafka")
        .option(
            "kafka.bootstrap.servers",
            KAFKA_BOOTSTRAP_SERVERS
        )
        .option(
            "topic",
            AUDIT_TOPIC
        )
        .option(
            "checkpointLocation",
            f"{CHECKPOINT_DIR}/audit_topic"
        )
        .outputMode("append")
        .start()
    )