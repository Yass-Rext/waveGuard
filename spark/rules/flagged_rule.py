from pyspark.sql.functions import when, col

def apply(df):
    return df.withColumn(
        "score_flagged",
        when(col("is_flagged"), 2).otherwise(0)
    )