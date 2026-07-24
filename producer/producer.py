"""
WaveGuard - Producteur Kafka
Simulation de transactions Mobile Money en temps réel
"""

# ==========================
# Importation des bibliothèques
# ==========================

import json
import random
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer
from faker import Faker
import logging


# ==========================================================
# Configuration du logger
# ==========================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

logger = logging.getLogger("WaveGuardProducer")


# ==========================================================
# Paramètres de la simulation
# ==========================================================

# Nombre total de comptes simulés
NUMBER_OF_ACCOUNTS = 50

# Taille d'une rafale frauduleuse
BURST_SIZE = 8

# Probabilité qu'une fraude soit déclenchée
FRAUD_PROBABILITY = 0.05

# Temps entre deux transactions normales (secondes)
NORMAL_TRANSACTION_DELAY = 0.5

# Temps entre deux transactions d'une rafale (secondes)
BURST_TRANSACTION_DELAY = 0.05

# Montants des transactions normales
MIN_NORMAL_AMOUNT = 500
MAX_NORMAL_AMOUNT = 150_000

# Montants des transactions frauduleuses
MIN_FRAUD_AMOUNT = 800_000
MAX_FRAUD_AMOUNT = 2_000_000



# ==========================
# Initialisation de Faker
# ==========================

fake = Faker("fr_FR")


# ==========================
# Configuration Kafka
# ==========================

# Depuis la machine hôte
BROKER = "localhost:9092"

# Si tu exécutes le producer depuis un conteneur Docker,
# il faudra utiliser :
# BROKER = "kafka:29092"

TOPIC = "transactions"


# ==========================
# Configuration du Producer
# ==========================

producer_config = {
    "bootstrap.servers": BROKER,

    # Fiabilité
    "acks": "all",

    # Nouvelle tentative en cas d'échec
    "retries": 5,

    # Attente maximale avant échec
    "message.timeout.ms": 30000,

    # Compression des messages
    "compression.type": "snappy"
}

producer = Producer(producer_config)


# ==========================
# Comptes simulés
# ==========================

ACCOUNTS = [
    f"SN_{i:04d}"
    for i in range(1, NUMBER_OF_ACCOUNTS + 1)
]

FRAUD_ACCOUNTS = [
    "SN_0042",
    "SN_0007",
    "SN_0013"
]

TRANSACTION_TYPES = [
    "P2P",
    "PAIEMENT_MARCHAND",
    "RETRAIT"
]

LOCATIONS = [
    "Dakar",
    "Thiès",
    "Saint-Louis",
    "Kaolack",
    "Ziguinchor"
]


# ==========================================================
# Génération d'une transaction Mobile Money
# ==========================================================

def generate_transaction(fraud: bool = False):
    """
    Génère une transaction Mobile Money.

    Parameters
    ----------
    fraud : bool
        True si la transaction provient d'un compte fraudeur.

    Returns
    -------
    dict
        Transaction au format JSON.
    """

    # Choix de l'expéditeur
    sender = random.choice(FRAUD_ACCOUNTS if fraud else ACCOUNTS)

    # Le destinataire doit être différent de l'expéditeur
    receiver = random.choice(
        [acc for acc in ACCOUNTS if acc != sender]
    )

    # Montant
    if fraud:
        amount = random.randint(MIN_FRAUD_AMOUNT, MAX_FRAUD_AMOUNT)
    else:
        amount = random.randint(MIN_NORMAL_AMOUNT, MAX_NORMAL_AMOUNT)

    transaction = {

        "transaction_id": str(uuid.uuid4()),

        "timestamp": datetime.now(
            timezone.utc
        ).isoformat(),

        "sender_id": sender,

        "receiver_id": receiver,

        "amount_fcfa": amount,

        "transaction_type": random.choice(
            TRANSACTION_TYPES
        ),

        "location": random.choice(
            LOCATIONS
        ),

        "is_flagged": fraud
    }

    return transaction


# ==========================================================
# Callback de confirmation d'envoi
# ==========================================================

def delivery_report(err, msg):
    """
    Callback appelé automatiquement par Kafka
    après la tentative d'envoi d'un message.
    """

    if err is not None:
        logger.error(f"Livraison échouée : {err}")
        return

    logger.info(
    f"Transaction envoyée | "
    f"Topic={msg.topic()} | "
    f"Partition={msg.partition()} | "
    f"Offset={msg.offset()} | "
    f"Sender={msg.key().decode('utf-8')}")


# ==========================================================
# Envoi d'une transaction vers Kafka
# ==========================================================

def send_transaction(transaction: dict):
    """
    Envoie une transaction vers Kafka.
    """

    producer.produce(
        topic=TOPIC,

        key=transaction["sender_id"],

        value=json.dumps(transaction).encode("utf-8"),

        callback=delivery_report,
    )

    # Déclenche les callbacks sans bloquer
    producer.poll(0)


# ==========================================================
# Simulation d'une attaque par vélocité
# ==========================================================

def send_fraud_burst(sender_id: str, burst_size: int = BURST_SIZE):
    """
    Envoie une rafale de transactions frauduleuses
    provenant du même compte.

    Parameters
    ----------
    sender_id : str
        Compte fraudeur.

    burst_size : int
        Nombre de transactions à envoyer.
    """

    logger.warning(
    f"Début d'un burst frauduleux pour {sender_id}")

    for i in range(burst_size):

        tx = generate_transaction(fraud=True)

        # On force l'expéditeur à rester le même
        tx["sender_id"] = sender_id

        send_transaction(tx)

        logger.info(f"Burst {i+1}/{burst_size} | "f"Sender={sender_id} | "
            f"Receiver={tx['receiver_id']} | "
            f"Amount={tx['amount_fcfa']} FCFA"
        )

        # 50 ms entre deux transactions
        time.sleep(BURST_TRANSACTION_DELAY)

    logger.warning(f"Burst terminé pour {sender_id}")



# ==========================================================
# Boucle principale
# ==========================================================

def main():
    """
    Lance la simulation des transactions Mobile Money.
    """

    logger.info("=" * 60)
    logger.info("WaveGuard - Producteur Kafka")
    logger.info("=" * 60)
    logger.info(f"Broker : {BROKER}")
    logger.info(f"Topic : {TOPIC}")
    logger.info("Simulation démarrée...")

    try:

        while True:

            # ----------------------------
            # Transaction normale
            # ----------------------------
            transaction = generate_transaction()

            send_transaction(transaction)

            logger.info(
                f"Transaction normale | "
                f"Sender={transaction['sender_id']} | "
                f"Receiver={transaction['receiver_id']} | "
                f"Amount={transaction['amount_fcfa']} FCFA"
            )

            # ----------------------------
            # Déclenchement aléatoire d'une fraude
            # ----------------------------
            if random.random() < FRAUD_PROBABILITY:

                fraud_sender = random.choice(FRAUD_ACCOUNTS)

                send_fraud_burst(fraud_sender)

            # ----------------------------
            # Pause entre deux transactions normales
            # ----------------------------
            time.sleep(NORMAL_TRANSACTION_DELAY)

    except KeyboardInterrupt:

        print("\nArrêt demandé par l'utilisateur...")

    finally:

        logger.info("Vidage du buffer Kafka...")

        producer.flush()

        logger.info("Producteur arrêté.")


if __name__ == "__main__":
    main()