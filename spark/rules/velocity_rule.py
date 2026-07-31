from pyspark.sql.functions import (
    col,
    count,
    window,
    when
)


def apply(df):

    return (
        df
        .groupBy(
            window(col("timestamp"), "5 minutes"),
            col("sender_id")
        )
        .count()
        .withColumn(
            "score_velocity",
            when(col("count") > 5, 1).otherwise(0)
        )
    )