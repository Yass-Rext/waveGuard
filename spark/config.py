APP_NAME = "WaveGuardDetector"

# Kafka
KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"

TRANSACTIONS_TOPIC = "transactions"
FRAUD_TOPIC = "fraud_alerts"
AUDIT_TOPIC = "audit_log"

# MinIO
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "password123"

NORMAL_PATH = "s3a://waveguard/normal"
FRAUD_PATH = "s3a://waveguard/fraud"
AUDIT_PATH = "s3a://waveguard/audit"

CHECKPOINT_DIR = "/home/jovyan/checkpoints"