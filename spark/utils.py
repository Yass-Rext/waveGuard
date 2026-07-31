# utils.py
from pyspark.sql.functions import from_json, col

from schemas import transaction_schema


def parse_transactions(raw_df):
    """
    Parse les messages JSON depuis Kafka
    """
    return (
        raw_df
        .selectExpr("CAST(value AS STRING)")
        .select(
            from_json(
                col("value"),
                transaction_schema
            ).alias("transaction")
        )
        .select("transaction.*")
    )


def fraud_transactions(df):
    """
    Filtre les transactions frauduleuses (alertes)
    Note: Cette fonction est utilisée pour le sink Parquet des fraudes
    """
    # Si df contient déjà les alertes (après apply_rules), on les retourne telles quelles
    # Sinon, on filtre sur le champ is_flagged ou risk_level
    if "fraud_type" in df.columns:
        return df  # Déjà des alertes
    else:
        # Fallback: filtre sur is_flagged si présent
        return df.filter(col("is_flagged") == True)


def normal_transactions(df):
    """
    Filtre les transactions normales (non-frauduleuses)
    """
    # Si on a un champ risk_level, on filtre
    if "risk_level" in df.columns:
        return df.filter(col("risk_level") != "HIGH")
    else:
        # Fallback: transactions non flaggées
        return df.filter(col("is_flagged") == False)