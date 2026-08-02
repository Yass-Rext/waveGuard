#!/usr/bin/env python3
# monitoring/metrics_exporter.py

import time
import json
import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as spark_sum, avg as spark_avg

# ============================================================
# CONFIGURATION
# ============================================================

METRICS_FILE = "/tmp/metrics/waveguard_metrics.json"
MINIO_BUCKET = "s3a://waveguard"
NORMAL_PATH = f"{MINIO_BUCKET}/normal"
FRAUD_PATH = f"{MINIO_BUCKET}/fraud"

MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "password123"

REFRESH_INTERVAL = 30

# ============================================================
# SPARK SESSION
# ============================================================

def create_spark_session():
    return (
        SparkSession.builder
        .appName("WaveGuard_MetricsExporter")
        .config("spark.jars.packages", 
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.262")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )

# ============================================================
# FONCTIONS DE MÉTRIQUES
# ============================================================

def safe_format_timestamp(ts):
    """Convertit un timestamp en chaîne ISO, quel que soit son type."""
    if ts is None:
        return None
    if hasattr(ts, 'isoformat'):
        return ts.isoformat()
    if isinstance(ts, str):
        return ts
    return str(ts)

def get_fraud_metrics(spark):
    try:
        df_fraud = spark.read.parquet(FRAUD_PATH)
        
        if df_fraud.count() == 0:
            return {
                "total_alerts": 0,
                "by_type": {},
                "top_fraudsters": [],
                "total_amount": 0,
                "avg_fraud_score": 0
            }
        
        total_alerts = df_fraud.count()
        
        # Alertes par type
        if "score_type" in df_fraud.columns:
            by_type = df_fraud.groupBy("score_type").count().collect()
            fraud_by_type = {row["score_type"]: row["count"] for row in by_type}
        else:
            by_type = df_fraud.groupBy("risk_level").count().collect()
            fraud_by_type = {row["risk_level"]: row["count"] for row in by_type}
        
        # Top 5 fraudeurs
        top_fraudsters = (
            df_fraud.groupBy("sender_id")
            .count()
            .orderBy(col("count").desc())
            .limit(5)
            .collect()
        )
        top_list = [
            {"sender": row["sender_id"], "alerts": row["count"]} 
            for row in top_fraudsters
        ]
        
        # Montant total
        total_amount = df_fraud.select(spark_sum("amount_fcfa")).collect()[0][0] or 0
        
        # Score moyen
        avg_score = 0
        if "fraud_score" in df_fraud.columns:
            avg_score = df_fraud.select(spark_avg("fraud_score")).collect()[0][0] or 0
        
        return {
            "total_alerts": total_alerts,
            "by_type": fraud_by_type,
            "top_fraudsters": top_list,
            "total_amount": total_amount,
            "avg_fraud_score": avg_score
        }
        
    except Exception as e:
        print(f"❌ Erreur lors de la lecture des fraudes : {e}")
        return {
            "total_alerts": 0, 
            "by_type": {}, 
            "top_fraudsters": [], 
            "total_amount": 0,
            "avg_fraud_score": 0
        }


def get_transaction_metrics(spark):
    try:
        df_normal = spark.read.parquet(NORMAL_PATH)
        
        if df_normal.count() == 0:
            return {
                "total_transactions": 0,
                "average_amount": 0,
                "total_amount": 0,
                "by_type": {},
                "by_location": {}
            }
        
        total = df_normal.count()
        total_amount = df_normal.select(spark_sum("amount_fcfa")).collect()[0][0] or 0
        avg_amount = total_amount / total if total > 0 else 0
        
        by_type = df_normal.groupBy("transaction_type").count().collect()
        tx_by_type = {row["transaction_type"]: row["count"] for row in by_type}
        
        by_location = df_normal.groupBy("location").count().collect()
        tx_by_location = {row["location"]: row["count"] for row in by_location}
        
        return {
            "total_transactions": total,
            "average_amount": avg_amount,
            "total_amount": total_amount,
            "by_type": tx_by_type,
            "by_location": tx_by_location
        }
        
    except Exception as e:
        print(f"❌ Erreur lors de la lecture des transactions : {e}")
        return {
            "total_transactions": 0, 
            "average_amount": 0, 
            "total_amount": 0, 
            "by_type": {},
            "by_location": {}
        }


def get_recent_activity(spark):
    try:
        df_fraud = spark.read.parquet(FRAUD_PATH)
        
        if df_fraud.count() == 0:
            return []
        
        recent = (
            df_fraud
            .orderBy(col("timestamp").desc())
            .limit(5)
            .select("sender_id", "amount_fcfa", "transaction_type", "timestamp", "location", "risk_level")
            .collect()
        )
        
        return [
            {
                "sender": row["sender_id"],
                "amount": row["amount_fcfa"],
                "type": row["transaction_type"],
                "timestamp": safe_format_timestamp(row["timestamp"]),
                "location": row["location"],
                "risk_level": row["risk_level"]
            }
            for row in recent
        ]
        
    except Exception as e:
        print(f"❌ Erreur lors de la récupération des alertes récentes : {e}")
        return []


def get_risk_distribution(spark):
    try:
        df_fraud = spark.read.parquet(FRAUD_PATH)
        
        if df_fraud.count() == 0:
            return {}
        
        distribution = df_fraud.groupBy("risk_level").count().collect()
        return {row["risk_level"]: row["count"] for row in distribution}
        
    except Exception as e:
        print(f"❌ Erreur lors de la distribution des risques : {e}")
        return {}


def get_alert_rate_metrics(fraud_metrics, tx_metrics):
    total_tx = tx_metrics["total_transactions"]
    total_alerts = fraud_metrics["total_alerts"]
    
    alert_rate = (total_alerts / (total_tx + total_alerts) * 100) if (total_tx + total_alerts) > 0 else 0
    
    return {
        "alert_rate": round(alert_rate, 2),
        "alerts_per_transaction": round(total_alerts / total_tx, 4) if total_tx > 0 else 0,
        "risk_score": min(100, int(alert_rate * 5))
    }

# ============================================================
# MÉTRIQUES PRINCIPALES
# ============================================================

def collect_all_metrics(spark):
    start_time_collect = time.time()
    
    fraud_metrics = get_fraud_metrics(spark)
    tx_metrics = get_transaction_metrics(spark)
    derived_metrics = get_alert_rate_metrics(fraud_metrics, tx_metrics)
    recent_activity = get_recent_activity(spark)
    risk_distribution = get_risk_distribution(spark)
    
    return {
        "timestamp": time.time(),
        "datetime": datetime.now().isoformat(),
        "fraud": fraud_metrics,
        "transactions": tx_metrics,
        "derived": derived_metrics,
        "recent_alerts": recent_activity,
        "risk_distribution": risk_distribution,
        "performance": {
            "collect_duration": round(time.time() - start_time_collect, 2)
        }
    }


def save_metrics(metrics, filepath=METRICS_FILE):
    try:
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        with open(filepath, 'w') as f:
            json.dump(metrics, f, indent=2, default=str)
        print(f"✅ Métriques sauvegardées dans {filepath}")
        print(f"   📊 Alertes: {metrics['fraud']['total_alerts']}")
        print(f"   📈 Transactions: {metrics['transactions']['total_transactions']}")
        print(f"   ⚠️  Taux d'alerte: {metrics['derived']['alert_rate']}%")
        return True
    except Exception as e:
        print(f"❌ Erreur lors de la sauvegarde : {e}")
        return False

# ============================================================
# BOUCLE PRINCIPALE
# ============================================================

def main():
    print("=" * 60)
    print("📊 WAVEGUARD - EXPORTATEUR DE MÉTRIQUES")
    print("=" * 60)
    print(f"📁 Fichier de sortie : {METRICS_FILE}")
    print(f"🔄 Intervalle : {REFRESH_INTERVAL} secondes")
    print(f"💾 Source : {MINIO_BUCKET}")
    print("=" * 60)
    
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    print("✅ Session Spark créée")
    
    try:
        while True:
            print(f"\n🔄 Collecte des métriques...")
            metrics = collect_all_metrics(spark)
            save_metrics(metrics)
            time.sleep(REFRESH_INTERVAL)
            
    except KeyboardInterrupt:
        print("\n🛑 Arrêt demandé par l'utilisateur")
    except Exception as e:
        print(f"❌ Erreur fatale : {e}")
    finally:
        spark.stop()
        print("✅ Exportateur arrêté")


if __name__ == "__main__":
    main()