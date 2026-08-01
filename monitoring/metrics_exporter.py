#!/usr/bin/env python3
# monitoring/metrics_exporter.py
"""
Exportateur de métriques pour WaveGuard.
Lit les données depuis MinIO et les expose sous forme de JSON pour Grafana.
"""

import time
import json
import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as spark_sum

# ============================================================
# CONFIGURATION
# ============================================================

# Où écrire les métriques pour Grafana
METRICS_FILE = "/tmp/waveguard_metrics.json"

# Chemins MinIO (doivent correspondre à config.py)
MINIO_BUCKET = "s3a://waveguard"
NORMAL_PATH = f"{MINIO_BUCKET}/normal"
FRAUD_PATH = f"{MINIO_BUCKET}/fraud"

# Configuration MinIO
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "password123"

# Intervalle de rafraîchissement (secondes)
REFRESH_INTERVAL = 30
