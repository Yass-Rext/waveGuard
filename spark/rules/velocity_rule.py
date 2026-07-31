from pyspark.sql.functions import (
    col,
    count,
    window
)

def apply(df):

    # 1. Calcul du nombre de transactions par fenêtre de 5 minutes
    velocity = (
        df
        .groupBy(
            window(col("timestamp"), "5 minutes"),
            col("sender_id")
        )
        .agg(
            count("*").alias("transaction_count")
        )
        .withColumn(
            "score_velocity",
            (col("transaction_count") > 5).cast("integer")
        )
    )

    # 2. Jointure Stream-Stream avec condition d'intervalle de temps
    joined_df = df.join(
        velocity,
        on=[
            df.sender_id == velocity.sender_id,
            df.timestamp >= velocity.window.start,
            df.timestamp <= velocity.window.end
        ],
        how="left_outer"
    )

    # 3. Traitement du résultat (remplacement des NUL/None par 0)
    return joined_df.fillna(0, subset=["score_velocity"])