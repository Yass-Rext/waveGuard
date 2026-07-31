# config.py
# import os

APP_NAME = "WaveGuardDetector"

# ============================================================
# Kafka
# ============================================================
KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"

TRANSACTIONS_TOPIC = "transactions"
FRAUD_TOPIC = "fraud-alerts"
AUDIT_TOPIC = "audit-log"

# ============================================================
# MinIO
# ============================================================
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "password123"

# Chemins S3 pour le Data Lake
NORMAL_PATH = "s3a://waveguard/normal"
FRAUD_PATH = "s3a://waveguard/fraud"
AUDIT_PATH = "s3a://waveguard/audit"

# ============================================================
# Checkpoints (gardés en local pour la performance)
# ============================================================
CHECKPOINT_DIR = "/tmp/waveguard_checkpoints"