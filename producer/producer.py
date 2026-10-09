"""Producteur de signes vitaux HealthStream ICU Monitor (séance 1).

Publie un relevé par patient et par intervalle sur le topic Kafka, avec le
patient_id comme clé de partitionnement (ordre chronologique garanti par patient).
"""

import json
import os
import random
import signal
import sys
import time

from confluent_kafka import Producer

# --- (1) Configuration lue dans l'environnement, défauts utilisables depuis l'hôte ---
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")
TOPIC = os.getenv("VITALS_TOPIC", "mimic-vitals")
NB_PATIENTS = int(os.getenv("NB_PATIENTS", "12"))
INTERVAL_S = float(os.getenv("INTERVAL_S", "1.0"))

# Plages physiologiques (min, max, unité)
PLAGES = {
    "HR": (55.0, 115.0, "bpm"),
    "SPO2": (90.0, 100.0, "%"),
    "ABP_SYS": (95.0, 145.0, "mmHg"),
}

_arret_demande = False
_non_livres = 0


def gerer_signal(signum, frame):
    """Lève le drapeau d'arrêt : la boucle principale sortira proprement."""
    global _arret_demande
    _arret_demande = True
    print(f"producteur: signal {signum} reçu, arrêt en cours...", file=sys.stderr)


def creer_producteur() -> Producer:
    """Construit le producteur et sa configuration de fiabilité."""
    config = {
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "client.id": "vitals-simulator-s1",
        "acks": "all",                    # leader + ISR : aucune perte acceptée
        "linger.ms": 5,                   # petit lot : latence quasi nulle à 12 msg/s
        "retries": 5,
        "enable.idempotence": True,       # pas de doublon ni d'inversion sur retry
        "partitioner": "murmur2_random",  # même hachage que le client Java (Flink)
    }
    return Producer(config)


def accuse_reception(err, msg):
    """Callback de livraison : ne trace que les échecs."""
    global _non_livres
    if err is not None:
        _non_livres += 1
        cle = msg.key().decode("utf-8") if msg.key() else None
        print(f"producteur: échec de livraison (clé={cle}) : {err}", file=sys.stderr)


def generer_releve(patient_id: str) -> dict:
    """Produit un relevé conforme au format HealthStream (section 4.1)."""
    vital_type = random.choice(list(PLAGES))
    vmin, vmax, unite = PLAGES[vital_type]
    return {
        "patient_id": patient_id,
        "event_time": int(time.time() * 1000),
        "vital_type": vital_type,
        "value": round(random.uniform(vmin, vmax), 1),
        "unit": unite,
    }


def main() -> int:
    signal.signal(signal.SIGINT, gerer_signal)
    signal.signal(signal.SIGTERM, gerer_signal)

    producteur = creer_producteur()
    patients = [f"P{i:03d}" for i in range(1, NB_PATIENTS + 1)]
    print(
        f"producteur: démarrage bootstrap={KAFKA_BOOTSTRAP} topic={TOPIC} "
        f"patients={NB_PATIENTS} intervalle={INTERVAL_S}s"
    )

    envoyes = 0
    debut = time.monotonic()

    # --- (3) Boucle de production ---
    while not _arret_demande:
        for patient_id in patients:
            releve = generer_releve(patient_id)
            try:
                producteur.produce(
                    topic=TOPIC,
                    key=patient_id.encode("utf-8"),
                    value=json.dumps(releve).encode("utf-8"),
                    on_delivery=accuse_reception,
                )
                envoyes += 1
            except BufferError:
                # File locale pleine (broker absent trop longtemps) : on laisse
                # le client se vider un peu avant de continuer.
                print("producteur: buffer local plein, attente...", file=sys.stderr)
                producteur.poll(1)
        producteur.poll(0)
        time.sleep(INTERVAL_S)

    # --- (4) Arrêt propre ---
    restants = producteur.flush(10)
    duree = time.monotonic() - debut
    debit = envoyes / duree if duree > 0 else 0.0
    print(
        f"producteur: bilan envoyés={envoyes} non_livrés={_non_livres + restants} "
        f"durée={duree:.1f}s débit={debit:.2f} msg/s"
    )
    return 0 if restants == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
