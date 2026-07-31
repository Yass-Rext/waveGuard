# waveguard_detector.py
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
# Spark avec MinIO
# =====================================================

spark = (
    SparkSession.builder
    .appName(APP_NAME)
    
    # Packages Kafka + Hadoop AWS pour MinIO
    .config("spark.jars.packages", 
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,"
            "org.apache.hadoop:hadoop-aws:3.3.4")
    
    # Configuration MinIO
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    
    # Pour améliorer la performance avec MinIO
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", 
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    
    # Checkpoint
    .config("spark.sql.streaming.checkpointLocation", "/tmp/waveguard_checkpoint")
    
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# =====================================================
# Lecture Kafka
# =====================================================

raw_transactions = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", TRANSACTIONS_TOPIC)
    .option("startingOffsets", "latest")
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
    .withColumn("timestamp", col("timestamp").cast("timestamp"))
    .withWatermark("timestamp", "2 minutes")
)

# =====================================================
# Application des règles
# =====================================================

fraud_alerts = apply_rules(transactions)

# =====================================================
# Séparation
# =====================================================

normal_df = normal_transactions(transactions)
fraud_df = fraud_transactions(fraud_alerts)
audit_df = transactions

# =====================================================
# Sinks
# =====================================================

fraud_topic_query = write_fraud_topic(fraud_alerts)
audit_topic_query = write_audit_topic(audit_df)

normal_query = write_normal_transactions(normal_df)
fraud_query = write_fraud_transactions(fraud_df)
audit_query = write_audit_logs(audit_df)

# Console pour debug
console_query = (
    fraud_alerts.writeStream
    .format("console")
    .option("truncate", False)
    .outputMode("update")
    .trigger(processingTime="10 seconds")
    .start()
)

# =====================================================
# Statut
# =====================================================

print("\n" + "="*60)
print("WAVEGUARD - ÉTAT DES STREAMS")
print("="*60)

queries = [
    ("📊 Normal (Parquet)", normal_query),
    ("🚨 Fraude (Parquet)", fraud_query),
    ("📋 Audit (Parquet)", audit_query),
    ("🚨 Alertes (Kafka)", fraud_topic_query),
    ("📋 Audit (Kafka)", audit_topic_query),
    ("🖥️ Console", console_query),
]

for name, query in queries:
    status = "✅ ACTIF" if query.isActive else "❌ ARRÊTÉ"
    print(f"{name:30} | {status}")
    if not query.isActive and query.exception():
        print(f"   Erreur: {query.exception()}")

print("\n⏳ En attente de la fin du streaming...\n")

try:
    spark.streams.awaitAnyTermination()
except KeyboardInterrupt:
    print("\n🛑 Arrêt demandé...")
    for _, query in queries:
        if query.isActive:
            query.stop()
    print("✅ Arrêt terminé.")