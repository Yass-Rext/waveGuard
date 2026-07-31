from pyspark.sql.functions import col, when

def apply(df):

    df = df.withColumn(
        "fraud_score",
        col("score_amount")
        + col("score_flagged")
        + col("score_type")
        + col("score_location")
        + col("score_velocity")
    )

    return df.withColumn(
        "risk_level",
        when(col("fraud_score") >= 3, "HIGH")
        .when(col("fraud_score") >= 1, "MEDIUM")
        .otherwise("LOW")
    )