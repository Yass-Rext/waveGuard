from pyspark.sql.functions import when, col

def apply(df):
    return df.withColumn(
        "score_location",
        when(col("location") != "Dakar", 1).otherwise(0)
    )