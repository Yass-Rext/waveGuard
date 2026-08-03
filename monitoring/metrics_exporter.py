#!/usr/bin/env python3
# monitoring/metrics_exporter_pg.py
"""
Exportateur de métriques vers PostgreSQL
Lit les données depuis MinIO et les écrit dans PostgreSQL pour Grafana.
"""

import time
import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as spark_sum, avg as spark_avg
import psycopg2
from psycopg2.extras import execute_values

# ============================================================
# CONFIGURATION
# ============================================================

MINIO_BUCKET = "s3a://waveguard"
NORMAL_PATH = f"{MINIO_BUCKET}/normal"
FRAUD_PATH = f"{MINIO_BUCKET}/fraud"

# Configuration PostgreSQL
PG_HOST = os.getenv("PG_HOST", "postgres")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_DATABASE = os.getenv("PG_DATABASE", "waveguard")
PG_USER = os.getenv("PG_USER", "waveguard")
PG_PASSWORD = os.getenv("PG_PASSWORD", "waveguard123")

# Configuration MinIO
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "password123")

REFRESH_INTERVAL = 30

# ============================================================
# CONNEXION POSTGRESQL
# ============================================================

def get_db_connection():
    """Crée une connexion à PostgreSQL."""
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        database=PG_DATABASE,
        user=PG_USER,
        password=PG_PASSWORD
    )

# ============================================================
# SPARK SESSION
# ============================================================

def create_spark_session():
    return (
        SparkSession.builder
        .appName("WaveGuard_MetricsExporterPG")
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
# FONCTIONS MÉTRIQUES
# ============================================================

def safe_format_timestamp(ts):
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
            return {"total_alerts": 0, "by_type": {}, "top_fraudsters": [], "total_amount": 0}
        
        total_alerts = df_fraud.count()
        
        # Alertes par type
        if "score_type" in df_fraud.columns:
            by_type = df_fraud.groupBy("score_type").count().collect()
            fraud_by_type = {row["score_type"]: row["count"] for row in by_type}
        else:
            by_type = df_fraud.groupBy("risk_level").count().collect()
            fraud_by_type = {row["risk_level"]: row["count"] for row in by_type}
        
        # Top fraudeurs
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
        
        total_amount = df_fraud.select(spark_sum("amount_fcfa")).collect()[0][0] or 0
        
        return {
            "total_alerts": total_alerts,
            "by_type": fraud_by_type,
            "top_fraudsters": top_list,
            "total_amount": total_amount
        }
        
    except Exception as e:
        print(f"❌ Erreur fraudes : {e}")
        return {"total_alerts": 0, "by_type": {}, "top_fraudsters": [], "total_amount": 0}


def get_transaction_metrics(spark):
    try:
        df_normal = spark.read.parquet(NORMAL_PATH)
        if df_normal.count() == 0:
            return {"total_transactions": 0, "average_amount": 0, "total_amount": 0, "by_type": {}}
        
        total = df_normal.count()
        total_amount = df_normal.select(spark_sum("amount_fcfa")).collect()[0][0] or 0
        avg_amount = total_amount / total if total > 0 else 0
        
        by_type = df_normal.groupBy("transaction_type").count().collect()
        tx_by_type = {row["transaction_type"]: row["count"] for row in by_type}
        
        return {
            "total_transactions": total,
            "average_amount": avg_amount,
            "total_amount": total_amount,
            "by_type": tx_by_type
        }
        
    except Exception as e:
        print(f"❌ Erreur transactions : {e}")
        return {"total_transactions": 0, "average_amount": 0, "total_amount": 0, "by_type": {}}


def get_recent_alerts(spark):
    try:
        df_fraud = spark.read.parquet(FRAUD_PATH)
        if df_fraud.count() == 0:
            return []
        
        recent = (
            df_fraud
            .orderBy(col("timestamp").desc())
            .limit(10)
            .select("sender_id", "amount_fcfa", "transaction_type", "timestamp", "location", "risk_level")
            .collect()
        )
        
        return [
            {
                "sender_id": row["sender_id"],
                "amount": row["amount_fcfa"],
                "type": row["transaction_type"],
                "timestamp": safe_format_timestamp(row["timestamp"]),
                "location": row["location"],
                "risk_level": row["risk_level"]
            }
            for row in recent
        ]
        
    except Exception as e:
        print(f"❌ Erreur alerts récentes : {e}")
        return []

# ============================================================
# SAUVEGARDE DANS POSTGRESQL
# ============================================================

def save_metrics_to_postgres(metrics):
    """Sauvegarde les métriques dans PostgreSQL."""
    conn = None
    cur = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # 1. Sauvegarder les métriques agrégées
        metrics_data = [
            ('fraud_alerts_total', metrics['fraud']['total_alerts'], 'counter'),
            ('transactions_total', metrics['transactions']['total_transactions'], 'counter'),
            ('fraud_amount_total', metrics['fraud']['total_amount'], 'counter'),
        ]
        
        # Ajouter l'alert_rate si disponible
        if 'derived' in metrics:
            metrics_data.append(('alert_rate', metrics['derived']['alert_rate'], 'gauge'))
        
        # Insertion des métriques
        execute_values(
            cur,
            """
            INSERT INTO metrics_aggregates (metric_name, metric_value, metric_type, recorded_at)
            VALUES %s
            """,
            [(name, value, mtype, datetime.now()) for name, value, mtype in metrics_data],
            page_size=100
        )
        
        # 2. Sauvegarder les alertes récentes (si pas déjà présentes)
        if 'recent_alerts' in metrics and metrics['recent_alerts']:
            alert_values = []
            for alert in metrics['recent_alerts']:
                # Vérifier si l'alerte existe déjà
                cur.execute(
                    "SELECT 1 FROM fraud_alerts WHERE sender_id = %s AND detected_at = %s",
                    (alert['sender_id'], alert['timestamp'])
                )
                if not cur.fetchone():
                    # Déterminer le type de fraude
                    fraud_type = "VOLUME_FRAUD" if alert['amount'] > 500000 else "VELOCITY_FRAUD"
                    alert_values.append((
                        alert['sender_id'],
                        fraud_type,
                        datetime.now(),
                        datetime.now(),
                        alert['amount'],
                        datetime.now(),
                        alert.get('risk_level', 'HIGH')
                    ))
            
            if alert_values:
                execute_values(
                    cur,
                    """
                    INSERT INTO fraud_alerts (sender_id, fraud_type, window_start, window_end, metric_value, detected_at, risk_level)
                    VALUES %s
                    """,
                    alert_values,
                    page_size=100
                )
        
        conn.commit()
        print(f"✅ Métriques sauvegardées dans PostgreSQL")
        print(f"   📊 Alertes: {metrics['fraud']['total_alerts']}")
        print(f"   📈 Transactions: {metrics['transactions']['total_transactions']}")
        
    except Exception as e:
        if conn:
            conn.rollback()
        print(f"❌ Erreur sauvegarde PostgreSQL : {e}")
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

# ============================================================
# BOUCLE PRINCIPALE
# ============================================================

def main():
    print("=" * 60)
    print("📊 WAVEGUARD - EXPORTATEUR POSTGRESQL")
    print("=" * 60)
    print(f"🔄 Intervalle : {REFRESH_INTERVAL} secondes")
    print(f"🐘 PostgreSQL : {PG_HOST}:{PG_PORT}/{PG_DATABASE}")
    print(f"💾 Source MinIO : {MINIO_BUCKET}")
    print("=" * 60)
    
    # Vérifier la connexion PostgreSQL
    try:
        conn = get_db_connection()
        conn.close()
        print("✅ Connexion PostgreSQL OK")
    except Exception as e:
        print(f"❌ Erreur connexion PostgreSQL : {e}")
        print("   Vérifiez que PostgreSQL est démarré")
        return
    
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    print("✅ Session Spark créée")
    
    try:
        while True:
            print(f"\n🔄 Collecte des métriques...")
            
            # Collecter les métriques
            fraud_metrics = get_fraud_metrics(spark)
            tx_metrics = get_transaction_metrics(spark)
            recent_alerts = get_recent_alerts(spark)
            
            # Calculer le taux d'alerte
            total_tx = tx_metrics["total_transactions"]
            total_alerts = fraud_metrics["total_alerts"]
            alert_rate = (total_alerts / (total_tx + total_alerts) * 100) if (total_tx + total_alerts) > 0 else 0
            
            # Construire le payload
            metrics = {
                "timestamp": time.time(),
                "datetime": datetime.now().isoformat(),
                "fraud": fraud_metrics,
                "transactions": tx_metrics,
                "derived": {"alert_rate": round(alert_rate, 2)},
                "recent_alerts": recent_alerts
            }
            
            # Sauvegarder dans PostgreSQL
            save_metrics_to_postgres(metrics)
            
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