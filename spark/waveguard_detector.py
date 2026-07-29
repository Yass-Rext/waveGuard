from pyspark.sql import SparkSession
from pyspark.sql.functions import (
  col,
    from_json,
    when,
    lit
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    BooleanType,
    TimestampType,
)


# ==========================================================
# Configuration
# ==========================================================

APP_NAME = "WaveGuardDetector"

KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"

INPUT_TOPIC = "transactions"

CHECKPOINT_DIR = "/home/jovyan/checkpoints"

OUTPUT_DIR = "/home/jovyan/data"


# ==========================================================
# Spark Session
# ==========================================================

spark = (
    SparkSession.builder
    .appName(APP_NAME)
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")


# ==========================================================
# Schéma des transactions
# ==========================================================

transaction_schema = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("timestamp", StringType(), False),
    StructField("sender_id", StringType(), False),
    StructField("receiver_id", StringType(), False),
    StructField("amount_fcfa", DoubleType(), False),
    StructField("transaction_type", StringType(), False),
    StructField("location", StringType(), False),
    StructField("is_flagged", BooleanType(), False),
])


# ==========================================================
# Lecture du topic Kafka
# ==========================================================

raw_transactions = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", INPUT_TOPIC)
    .option("startingOffsets", "latest")
    .load()
)


# ==========================================================
# Parsing des transactions
# ==========================================================

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
)


# ==========================================================
# Calcul du score de risque
# ==========================================================

transactions = (
    transactions

    # Règle 1
    .withColumn(
        "score_amount",
        when(col("amount_fcfa") > 500000, 1).otherwise(0)
    )

    # Règle 2
    .withColumn(
        "score_flagged",
        when(col("is_flagged") == True, 2).otherwise(0)
    )

    # Règle 3
    .withColumn(
        "score_type",
        when(col("transaction_type") == "international", 1).otherwise(0)
    )

    # Règle 4
    .withColumn(
        "score_location",
        when(col("location") != "Dakar", 1).otherwise(0)
    )
)

transactions = transactions.withColumn(
    "fraud_score",
    col("score_amount")
    + col("score_flagged")
    + col("score_type")
    + col("score_location")
)

transactions = transactions.withColumn(
    "risk_level",
    when(col("fraud_score") >= 3, "HIGH")
    .when(col("fraud_score") >= 1, "MEDIUM")
    .otherwise("LOW")
)

query = (
    transactions.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", False)
    .start()
)

query.awaitTermination()