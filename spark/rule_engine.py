# rule_engine.py - Version conforme au sujet
from pyspark.sql.functions import (
    window, count, sum as spark_sum, col, lit, current_timestamp
)


def apply_rules(df):
    """
    Applique les deux règles de détection de fraude :
    1. Vélocité : >5 transactions en 5 minutes (slide 1 min)
    2. Volume : >500 000 FCFA en 10 minutes (slide 2 min)
    """
    
    # =====================================================
    # RÈGLE 1 : Fraude par vélocité
    # =====================================================
    velocity_fraud = (
        df.groupBy(
            window(col("timestamp"), "5 minutes", "1 minute"),
            col("sender_id")
        )
        .agg(count("*").alias("tx_count"))
        .filter(col("tx_count") > 5)
        .select(
            col("sender_id"),
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("tx_count"),
            lit("VELOCITY_FRAUD").alias("fraud_type"),
            current_timestamp().alias("detected_at")
        )
    )
    
    # =====================================================
    # RÈGLE 2 : Fraude par volume
    # =====================================================
    volume_fraud = (
        df.groupBy(
            window(col("timestamp"), "10 minutes", "2 minutes"),
            col("sender_id")
        )
        .agg(spark_sum("amount_fcfa").alias("total_amount"))
        .filter(col("total_amount") > 500000)
        .select(
            col("sender_id"),
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("total_amount"),
            lit("VOLUME_FRAUD").alias("fraud_type"),
            current_timestamp().alias("detected_at")
        )
    )
    
    # Union des deux détections
    return velocity_fraud.union(volume_fraud)