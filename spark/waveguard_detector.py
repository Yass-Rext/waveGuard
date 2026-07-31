from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json

from config import *
from schemas import transaction_schema
from rule_engine import apply_rules
from storage import (
    write_normal_transactions,
    write_fraud_transactions,
    write_audit_logs,
)
from utils import (
    fraud_transactions,
    normal_transactions,
)

from kafka_writer import (
    write_fraud_topic,
    write_audit_topic,
)

# =====================================================
# Spark
# =====================================================

spark = (
    SparkSession.builder
    .appName(APP_NAME)

    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")

    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# =====================================================
# Lecture Kafka
# =====================================================

raw_transactions = (
    spark.readStream
    .format("kafka")
    .option(
        "kafka.bootstrap.servers",
        KAFKA_BOOTSTRAP_SERVERS
    )
    .option(
        "subscribe",
        TRANSACTIONS_TOPIC
    )
    .option(
        "startingOffsets",
        "latest"
    )
    .load()
)

transactions = (
    raw_transactions
    .selectExpr("CAST(value AS STRING)")
    .select(
        from_json(
            col("value"),
            transaction_schema
        ).alias("transaction")
    )
    .select("transaction.*")
    # 1. Convertir la colonne timestamp en vrai type Timestamp PySpark
    .withColumn("timestamp", col("timestamp").cast("timestamp"))
    # 2. Définir le Watermark (ex: 10 minutes de retard toléré)
    .withWatermark("timestamp", "10 minutes")
)

# =====================================================
# Application des règles
# =====================================================

transactions = apply_rules(transactions)

# =====================================================
# Séparation
# =====================================================

normal_df = normal_transactions(transactions)

fraud_df = fraud_transactions(transactions)

audit_df = transactions

fraud_topic_query = write_fraud_topic(fraud_df)

audit_topic_query = write_audit_topic(audit_df)

# =====================================================
# Sauvegarde MinIO
# =====================================================

normal_query = write_normal_transactions(normal_df)

fraud_query = write_fraud_transactions(fraud_df)

audit_query = write_audit_logs(audit_df)

# =====================================================
# Console
# =====================================================

console_query = (
    transactions.writeStream
    .format("console")
    .option("truncate", False)
    .outputMode("append")
    .start()
)

console_query.awaitTermination()