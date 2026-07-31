from pyspark.sql.functions import from_json, col

from schemas import transaction_schema

def parse_transactions(raw_df):

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

    return df.filter(
        col("risk_level") == "HIGH"
    )


def normal_transactions(df):

    return df.filter(
        col("risk_level") != "HIGH"
    )