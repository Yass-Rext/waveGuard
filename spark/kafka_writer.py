from pyspark.sql.functions import to_json, struct

from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    FRAUD_TOPIC,
    AUDIT_TOPIC,
    CHECKPOINT_DIR
)


def write_fraud_topic(df):
    """
    Écrit les alertes de fraude dans le topic Kafka 'fraud-alerts'
    OutputMode: UPDATE (car les fenêtres se mettent à jour)
    """
    kafka_df = df.select(to_json(struct("*")).alias("value"))
    
    return (
        kafka_df
        .writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("topic", FRAUD_TOPIC)
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/fraud_topic")
        .outputMode("update")  # ⚠️ Changé de "append" à "update"
        .trigger(processingTime="30 seconds")
        .start()
    )


def write_audit_topic(df):
    """
    Écrit les logs d'audit dans le topic Kafka 'audit-log'
    OutputMode: APPEND (on ajoute des lignes)
    """
    kafka_df = df.select(to_json(struct("*")).alias("value"))
    
    return (
        kafka_df
        .writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("topic", AUDIT_TOPIC)
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/audit_topic")
        .outputMode("append")  # ✅ Correct
        .trigger(processingTime="30 seconds")
        .start()
    )