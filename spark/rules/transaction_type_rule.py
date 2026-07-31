from pyspark.sql.functions import when, col

def apply(df):
    return df.withColumn(
        "score_type",
        when(col("transaction_type") == "international", 1).otherwise(0)
    )