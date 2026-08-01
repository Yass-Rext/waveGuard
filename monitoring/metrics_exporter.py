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


# ============================================================
# SPARK SESSION
# ============================================================

def create_spark_session():
    """
    Crée une session Spark pour lire les données Parquet dans MinIO.
    """
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

def get_fraud_metrics(spark):
    """
    Calcule les métriques de fraude à partir des alertes stockées.
    """
    try:
        # Lire les alertes de fraude
        df_fraud = spark.read.parquet(FRAUD_PATH)
        
        if df_fraud.count() == 0:
            return {
                "total_alerts": 0,
                "by_type": {},
                "top_fraudsters": [],
                "total_amount": 0
            }
        
        # Nombre total d'alertes
        total_alerts = df_fraud.count()
        
        # Alertes par type (VELOCITY_FRAUD, VOLUME_FRAUD)
        by_type = (
            df_fraud.groupBy("fraud_type")
            .count()
            .collect()
        )
        fraud_by_type = {row["fraud_type"]: row["count"] for row in by_type}
        
        # Top 5 fraudeurs (par nombre d'alertes)
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
        
        # Montant total frauduleux (pour les alertes VOLUME)
        total_amount = df_fraud.select(spark_sum("total_amount")).collect()[0][0] or 0
        
        return {
            "total_alerts": total_alerts,
            "by_type": fraud_by_type,
            "top_fraudsters": top_list,
            "total_amount": total_amount
        }
        
    except Exception as e:
        print(f"❌ Erreur lors de la lecture des fraudes : {e}")
        return {"total_alerts": 0, "by_type": {}, "top_fraudsters": [], "total_amount": 0}



def get_transaction_metrics(spark):
    """
    Calcule les métriques des transactions normales.
    """
    try:
        df_normal = spark.read.parquet(NORMAL_PATH)
        
        if df_normal.count() == 0:
            return {
                "total_transactions": 0,
                "average_amount": 0,
                "total_amount": 0,
                "by_type": {}
            }
        
        # Statistiques de base
        total = df_normal.count()
        total_amount = df_normal.select(spark_sum("amount_fcfa")).collect()[0][0] or 0
        avg_amount = total_amount / total if total > 0 else 0
        
        # Transactions par type (P2P, RETRAIT, etc.)
        by_type = (
            df_normal.groupBy("transaction_type")
            .count()
            .collect()
        )
        tx_by_type = {row["transaction_type"]: row["count"] for row in by_type}
        
        return {
            "total_transactions": total,
            "average_amount": avg_amount,
            "total_amount": total_amount,
            "by_type": tx_by_type
        }
        
    except Exception as e:
        print(f"❌ Erreur lors de la lecture des transactions : {e}")
        return {"total_transactions": 0, "average_amount": 0, "total_amount": 0, "by_type": {}}


def get_alert_rate_metrics(fraud_metrics, tx_metrics):
    """
    Calcule le taux d'alerte et d'autres métriques dérivées.
    """
    total_tx = tx_metrics["total_transactions"]
    total_alerts = fraud_metrics["total_alerts"]
    
    # Taux d'alertes (pourcentage)
    alert_rate = (total_alerts / (total_tx + total_alerts) * 100) if (total_tx + total_alerts) > 0 else 0
    
    # Alertes par minute (estimation basée sur le temps de vie du pipeline)
    # On pourrait ajouter un timestamp pour calculer le débit
    
    return {
        "alert_rate": round(alert_rate, 2),
        "alerts_per_transaction": round(total_alerts / total_tx, 4) if total_tx > 0 else 0,
        "risk_score": min(100, int(alert_rate * 5))  # Score de risque simplifié
    }


def get_recent_activity(spark):
    """
    Récupère les 5 dernières alertes pour affichage en temps réel.
    """
    try:
        df_fraud = spark.read.parquet(FRAUD_PATH)
        
        if df_fraud.count() == 0:
            return []
        
        # Trier par timestamp décroissant et prendre les 5 dernières
        recent = (
            df_fraud
            .orderBy(col("detected_at").desc())
            .limit(5)
            .select("sender_id", "fraud_type", "detected_at", "window_start", "window_end")
            .collect()
        )
        
        return [
            {
                "sender": row["sender_id"],
                "type": row["fraud_type"],
                "detected_at": row["detected_at"].isoformat() if row["detected_at"] else None,
                "window_start": row["window_start"].isoformat() if row["window_start"] else None,
                "window_end": row["window_end"].isoformat() if row["window_end"] else None
            }
            for row in recent
        ]
        
    except Exception as e:
        print(f"❌ Erreur lors de la récupération des alertes récentes : {e}")
        return []


# ============================================================
# MÉTRIQUES PRINCIPALES
# ============================================================

def collect_all_metrics(spark):
    """
    Collecte toutes les métriques et les retourne sous forme de dictionnaire.
    """
    # Métriques de fraude
    fraud_metrics = get_fraud_metrics(spark)
    
    # Métriques de transactions
    tx_metrics = get_transaction_metrics(spark)
    
    # Métriques dérivées
    derived_metrics = get_alert_rate_metrics(fraud_metrics, tx_metrics)
    
    # Alertes récentes
    recent_alerts = get_recent_activity(spark)
    
    # Métriques de performance (à ajouter selon vos besoins)
    performance_metrics = {
        "last_update": datetime.now().isoformat(),
        "uptime_seconds": int(time.time() - start_time) if 'start_time' in globals() else 0
    }
    
    return {
        "timestamp": time.time(),
        "datetime": datetime.now().isoformat(),
        "fraud": fraud_metrics,
        "transactions": tx_metrics,
        "derived": derived_metrics,
        "recent_alerts": recent_alerts,
        "performance": performance_metrics
    }


def save_metrics(metrics, filepath=METRICS_FILE):
    """
    Sauvegarde les métriques au format JSON pour Grafana.
    """
    try:
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
    """
    Boucle principale d'exportation des métriques.
    """
    global start_time
    start_time = time.time()
    
    print("=" * 60)
    print("📊 WAVEGUARD - EXPORTATEUR DE MÉTRIQUES")
    print("=" * 60)
    print(f"📁 Fichier de sortie : {METRICS_FILE}")
    print(f"🔄 Intervalle : {REFRESH_INTERVAL} secondes")
    print(f"💾 Source : {MINIO_BUCKET}")
    print("=" * 60)
    
    # Créer la session Spark
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    print("✅ Session Spark créée")
    
    # Créer le dossier parent si nécessaire
    os.makedirs(os.path.dirname(METRICS_FILE), exist_ok=True)
    
    try:
        while True:
            print(f"\n🔄 Collecte des métriques...")
            
            # Collecter toutes les métriques
            metrics = collect_all_metrics(spark)
            
            # Sauvegarder
            save_metrics(metrics)
            
            # Attendre le prochain cycle
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
