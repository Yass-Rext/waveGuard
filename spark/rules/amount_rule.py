from pyspark.sql.functions import when, col

def apply(df):
    return df.withColumn(
        "score_amount",
        when(col("amount_fcfa") > 500000, 1).otherwise(0)
    )