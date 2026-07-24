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