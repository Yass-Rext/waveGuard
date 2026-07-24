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
    for i in range(1, 51)
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
        amount = random.randint(800_000, 2_000_000)
    else:
        amount = random.randint(500, 150_000)

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
        print(f"[ERREUR] Livraison échouée : {err}")
        return

    print(
        f"[OK] "
        f"Topic={msg.topic()} | "
        f"Partition={msg.partition()} | "
        f"Offset={msg.offset()} | "
        f"Key={msg.key().decode('utf-8')}"
    )


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

def send_fraud_burst(sender_id: str, burst_size: int = 8):
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

    print(f"\n Début d'un burst frauduleux pour {sender_id}")

    for i in range(burst_size):

        tx = generate_transaction(fraud=True)

        # On force l'expéditeur à rester le même
        tx["sender_id"] = sender_id

        send_transaction(tx)

        print(
            f"   [{i+1}/{burst_size}] "
            f"{sender_id} -> "
            f"{tx['receiver_id']} | "
            f"{tx['amount_fcfa']} FCFA"
        )

        # 50 ms entre deux transactions
        time.sleep(0.05)

    print(f" Burst terminé pour {sender_id}\n")



# ==========================================================
# Boucle principale
# ==========================================================

def main():
    """
    Lance la simulation des transactions Mobile Money.
    """

    print("=" * 60)
    print("WaveGuard - Producteur Kafka")
    print("=" * 60)
    print(f"Broker : {BROKER}")
    print(f"Topic  : {TOPIC}")
    print("Simulation démarrée...\n")

    try:

        while True:

            # ----------------------------
            # Transaction normale
            # ----------------------------
            transaction = generate_transaction()

            send_transaction(transaction)

            print(
                f"[NORMAL] "
                f"{transaction['sender_id']} -> "
                f"{transaction['receiver_id']} | "
                f"{transaction['amount_fcfa']} FCFA"
            )

            # ----------------------------
            # Déclenchement aléatoire d'une fraude
            # ----------------------------
            if random.random() < 0.05:

                fraud_sender = random.choice(FRAUD_ACCOUNTS)

                send_fraud_burst(fraud_sender)

            # ----------------------------
            # Pause entre deux transactions normales
            # ----------------------------
            time.sleep(0.5)

    except KeyboardInterrupt:

        print("\nArrêt demandé par l'utilisateur...")

    finally:

        print("Vidage du buffer Kafka...")

        producer.flush()

        print("Producteur arrêté.")