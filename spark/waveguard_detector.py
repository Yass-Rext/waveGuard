from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
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