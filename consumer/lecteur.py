"""Lecteur exploratoire HealthStream (séance 2, parties 2 et 3).

N'écrit nulle part : lit mimic-vitals, compte par partition et affiche les
attributions / retraits de partitions pour observer offsets et rééquilibrage.
"""

import json
import os
import signal
import sys
import time

from confluent_kafka import Consumer, KafkaError

# --- Configuration par l'environnement ---
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")
TOPIC = os.getenv("VITALS_TOPIC", "mimic-vitals")
GROUP_ID = os.getenv("CONSUMER_GROUP", "explo-1")
OFFSET_RESET = os.getenv("AUTO_OFFSET_RESET", "earliest")
AUTO_COMMIT = os.getenv("AUTO_COMMIT", "true").strip().lower() in ("1", "true", "yes", "oui")
INSTANCE = os.getenv("INSTANCE", "c1")
# range par défaut ; cooperative-sticky pour la partie 3.4
STRATEGY = os.getenv("PARTITION_STRATEGY", "range")

COMMIT_TOUS_LES = 100
AFFICHE_TOUS_LES = 50

_arret_demande = False
lus = 0
par_partition = {}


def gerer_signal(signum, frame):
    """Lève le drapeau : la boucle sort d'elle-même et close() est appelé."""
    global _arret_demande
    _arret_demande = True


def creer_consommateur() -> Consumer:
    return Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": GROUP_ID,
        "client.id": f"lecteur-{INSTANCE}",
        "auto.offset.reset": OFFSET_RESET,
        "enable.auto.commit": AUTO_COMMIT,
        "partition.assignment.strategy": STRATEGY,
    })


def _horodatage() -> str:
    return time.strftime("%H:%M:%S")


def sur_attribution(consommateur, partitions):
    liste = ", ".join(str(p.partition) for p in partitions) or "aucune"
    print(f"[{INSTANCE}] ATTRIBUTION t={_horodatage()} lus={lus} "
          f"partitions=[{liste}]", flush=True)


def sur_retrait(consommateur, partitions):
    liste = ", ".join(str(p.partition) for p in partitions) or "aucune"
    print(f"[{INSTANCE}] RETRAIT t={_horodatage()} lus={lus} "
          f"partitions=[{liste}]", flush=True)


def main() -> int:
    global lus
    signal.signal(signal.SIGINT, gerer_signal)
    signal.signal(signal.SIGTERM, gerer_signal)

    consommateur = creer_consommateur()
    # subscribe() n'attribue rien : l'attribution arrive au premier poll().
    consommateur.subscribe([TOPIC], on_assign=sur_attribution, on_revoke=sur_retrait)
    print(f"[{INSTANCE}] démarrage groupe={GROUP_ID} reset={OFFSET_RESET} "
          f"auto_commit={AUTO_COMMIT} strategie={STRATEGY}", flush=True)

    try:
        while not _arret_demande:
            msg = consommateur.poll(1.0)  # timeout en SECONDES
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"[{INSTANCE}] erreur Kafka : {msg.error()}", file=sys.stderr, flush=True)
                continue
            try:
                json.loads(msg.value())
            except (ValueError, TypeError):
                print(f"[{INSTANCE}] message illisible p={msg.partition()} o={msg.offset()}",
                      file=sys.stderr, flush=True)
            lus += 1
            par_partition[msg.partition()] = par_partition.get(msg.partition(), 0) + 1
            if lus % AFFICHE_TOUS_LES == 0:
                print(f"[{INSTANCE}] lus={lus} dernier p={msg.partition()} "
                      f"o={msg.offset()} cle={msg.key().decode() if msg.key() else None}",
                      flush=True)
            if not AUTO_COMMIT and lus % COMMIT_TOUS_LES == 0:
                consommateur.commit(asynchronous=False)
    finally:
        repartition = ", ".join(f"{p}:{n}" for p, n in sorted(par_partition.items()))
        print(f"[{INSTANCE}] Arret. lus={lus} repartition={{{repartition}}}", flush=True)
        # close() quitte explicitement le groupe : rééquilibrage immédiat des autres.
        consommateur.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
