from pyspark.sql.types import *

transaction_schema = StructType([
    StructField("transaction_id", StringType()),
    StructField("timestamp", StringType()),
    StructField("sender_id", StringType()),
    StructField("receiver_id", StringType()),
    StructField("amount_fcfa", DoubleType()),
    StructField("transaction_type", StringType()),
    StructField("location", StringType()),
    StructField("is_flagged", BooleanType())
])