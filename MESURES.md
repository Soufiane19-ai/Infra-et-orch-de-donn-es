# MESURES — Séance 1 · HealthStream ICU Monitor

> Valeurs relevées le 2026-10-08 sur Windows 10 Pro + Docker Desktop (WSL 2).

## Versions du poste

| Outil | Version |
|---|---|                                                         
| Docker Engine | 28.5.1 |
| Docker Compose | v2.40.3 |
| Python | Python | 3.14.8 (poste) — runs hôte en 3.12.6 via `py -3.12`, image en 3.12.15 |

## Décisions retenues

| Décision | Valeur | Justification |
|---|---|---|
| Image du producteur | `vitals-producer:0.1` | tag explicite, jamais `latest` |
| WORKDIR | `/app` | chemin absolu court, cohérent avec le montage `/app/logs` |
| Volume nommé des journaux | `vitals-logs` | |
| Réseau applicatif | `vitals-net` (bridge) | réutilisé jusqu'à la séance 6 |
| `VITALS_TOPIC` | `mimic-vitals` | imposé par le projet |
| Healthcheck producer `interval` | `15s` | test trivial ; plus court chargerait le conteneur pour rien |
| Healthcheck producer `retries` | `3` | un service mort passe `unhealthy` en ~45 s max, sans flapper sur un échec isolé |
| Port hôte publié (→ 29092) | `29092` | même numéro des deux côtés, aucune ambiguïté |
| `KAFKA_PROCESS_ROLES` | `broker,controller` | un seul nœud KRaft porte les deux rôles |
| Facteur de réplication topics internes | `1` | un seul broker |
| `KAFKA_AUTO_CREATE_TOPICS_ENABLE` | `false` | une faute de frappe sur un topic échoue au lieu de créer un topic fantôme |
| Volume nommé Kafka | `vitals-kafka-data` | les journaux survivent à `down` |
| Partitions du topic | `3` | parallélisme minimal visible, sera augmenté en séance 2 |
| Replication factor du topic | `1` | un seul broker |
| `acks` | `all` | données médicales : on exige l'écriture par le leader et tout l'ISR (requis aussi par l'idempotence) |
| `linger.ms` | `5` | à ~12 msg/s, attendre plus n'apporte rien au débit et retarde les alertes |
| `partitioner` | `murmur2_random` | même hachage murmur2 que le client Java : une clé tombe dans la même partition côté Python et côté Flink (le défaut `consistent_random` utilise CRC32, non interopérable) |
| `NB_PATIENTS` | `12` | 12 lits |
| `INTERVAL_S` | `1.0` | un moniteur ICU échantillonne ~1 Hz |

## Partie 1 — Docker

**Question 1.1** — Non, le second conteneur affiche « fichier absent » : chaque conteneur a sa propre couche inscriptible au-dessus de l'image en lecture seule, et ce qu'écrit `c1` n'existe que dans la couche de `c1`.

**Mesure 1**

| Grandeur | Valeur |
|---|---|
| Taille de `python:3.12-slim` | 177 Mo |
| Taille de l'image finale | 193 Mo (+16 Mo par rapport à la base) |
| Couches non vides (`docker history`) | 9 : 4 héritées de la base + 5 ajoutées (`WORKDIR` 8,19 kB, `COPY requirements.txt` 12,3 kB, `pip install` 11 Mo, `COPY producer.py` 12,3 kB, `useradd/chown` 65,5 kB) |
| Couche la plus volumineuse | Base Debian trixie : 87,7 Mo ; parmi nos couches : `RUN pip install …` avec 11 Mo (roue confluent-kafka, librdkafka embarquée) |

**Mesure 2**

| Scénario | Durée / étapes CACHED |
|---|---|
| Aucune modification | 0,98 s — 5/5 étapes CACHED (`WORKDIR` → `RUN useradd`) |
| Modification de `producer.py` | 1,49 s — 3/5 CACHED (`WORKDIR`, `COPY requirements.txt`, `pip install`) ; rejoués : `COPY producer.py` + `RUN useradd` (0,3 s) |
| Modification de `requirements.txt` | 5,18 s — 1/5 CACHED (`WORKDIR`) ; cache rompu au `COPY requirements.txt`, `pip install` rejoué (3,0 s) : le plus lent |

**Question 1.2** — Le scénario « modification de `producer.py` » changerait : avec `COPY . ./` avant l'installation, toute modification du code invaliderait la couche copiée et donc le `pip install` qui suit. Le rebuild deviendrait aussi lent que celui après modification de `requirements.txt` (les deux autres scénarios restent inchangés).

## Partie 2 — Compose

Noms réels relevés : réseau `s1-vitals-stream_vitals-net`, volume `s1-vitals-stream_vitals-logs` (et `s1-vitals-stream_vitals-kafka-data`), Mountpoint `/var/lib/docker/volumes/s1-vitals-stream_vitals-logs/_data` (dans la VM Docker Desktop, pas sur le disque Windows). `nslookup producer` depuis un conteneur busybox attaché au réseau : résolu par le DNS embarqué `127.0.0.11` en `172.18.0.3`.

**Question 2.1** — (1) Le volume nommé est géré par Docker et portable : il ne dépend ni de l'arborescence ni de l'OS de l'hôte, donc le fichier Compose fonctionne à l'identique sur tous les postes (et plus tard sur Kubernetes, via un PVC). (2) Il évite les problèmes de droits : le conteneur tourne en UID 10001, un dossier `./logs` de l'hôte appartiendrait à un autre UID et serait non inscriptible, alors que le volume nommé hérite du propriétaire défini dans l'image. Bonus : meilleures performances sur Docker Desktop (pas de partage de fichiers hôte ↔ VM).

**Mesure 3** — délai observé avant `healthy` = 5,1 s après le démarrage du conteneur (7,9 s après la commande `up`, Kafka étant déjà healthy). Plus court que l'`interval` de 15 s : depuis Docker Engine 25, pendant la `start_period` les tests sont lancés toutes les `start_interval` (5 s par défaut), et le premier succès fait passer le conteneur `healthy` immédiatement.

## Partie 3 — Kafka KRaft

**Mesure 4** — temps entre `up -d kafka` et `healthy` : 16,1 s (volume vierge ; même mécanisme de `start_interval` à 5 s : le 3ᵉ test, vers 15 s, réussit).

**Question 3.1** — Le facteur de réplication ne peut dépasser le nombre de brokers, car deux répliques d'une même partition ne peuvent pas résider sur le même broker. Avec un seul broker, Kafka refuse la création avec une `InvalidReplicationFactorException`. Message exact observé (KRaft, cp-kafka 7.8.0) : « Unable to replicate the partition 2 time(s): The target replication factor of 2 cannot be reached because only 1 broker(s) are registered. »

**Mesure 5**

| Clé | Partition(s) et offsets |
|---|---|
| P001 | partition 2, offsets 0, 1, 2 |
| P002 | partition 1, offset 0 |
| P003 | partition 0, offset 0 |

Bornes (`GetOffsetShell --time -1`, classe `org.apache.kafka.tools.GetOffsetShell` dans Kafka 3.x) : `mimic-vitals:0:1`, `mimic-vitals:1:1`, `mimic-vitals:2:3` — somme = 5 (topic recréé vide juste avant l'envoi, car le producteur y avait déjà écrit au démarrage de la stack).

**Question 3.2** — Oui, les trois messages de P001 sont dans la même partition, car la partition est déterminée par le hachage de la clé. Leurs offsets sont croissants dans l'ordre d'envoi : ici 0, 1, 2, consécutifs car aucun autre patient ne partage la partition 2 (ils ne le seraient pas si un patient de la même partition s'intercalait). On en déduit que l'ordre est garanti par clé — donc par patient — mais pas entre patients de partitions différentes.

## Partie 4 — Producteur

**Mesure 6**

| Forme de `depends_on` | Erreurs de connexion au démarrage ? | Lignes d'erreur avant stabilisation |
|---|---|---|
| `depends_on: [kafka]` | Oui : `%3\|FAIL\|… kafka:9092/bootstrap: Connect to ipv4#172.18.0.2:9092 failed: Connection refused (after 1ms in state CONNECT)` | 2 (la 2ᵉ avec « 1 identical error(s) suppressed »), suivies d'un avertissement `%4\|GETPID … Coordinator load in progress: retrying` |
| `condition: service_healthy` | Non | 0 (seul subsiste l'avertissement `%4\|GETPID … Coordinator load in progress` : le broker répond mais charge encore son coordinateur ; aucune perte) |

## Partie 5 — Vérification

**Mesure 7**

| Grandeur | Valeur |
|---|---|
| Somme offsets t0 | 1661 |
| Somme offsets t1 (+60 s) | 2393 (Δ = 732 messages en 61,7 s réelles entre les deux relevés) |
| Débit calculé | 11,86 msg/s (732 / 61,7 ; précision limitée : `GetOffsetShell` met ~1,5 s à lancer sa JVM et les envois arrivent par paquets de 12) |
| Débit annoncé par le producteur | 11,99 msg/s (`envoyés=864 non_livrés=0 durée=72.1s`) |
| `INTERVAL_S` / `NB_PATIENTS` | 1.0 / 12 (théorique : 12 msg/s) |
| Débit avec `INTERVAL_S=0.1` | 119,09 msg/s annoncé (`envoyés=8580 non_livrés=0 durée=72.0s`) ; 119,4 msg/s par les offsets (Δ = 7380 en 61,8 s) |
| Écart à la théorie (120 msg/s) | −0,91 msg/s, soit −0,76 % (contre −0,08 % à 1 s) |

**Question 5.1** — Le débit mesuré est légèrement inférieur à `NB_PATIENTS / INTERVAL_S` : la période réelle d'un tour vaut `INTERVAL_S` + le temps d'exécution du tour (génération, `produce`, `poll`), puisque `sleep` s'ajoute au travail au lieu de le compenser. Mesuré ici (Python 3.12 natif sous Windows) : −0,08 % à 1 s, −0,76 % à 0,1 s. L'écart est environ dix fois plus grand car le surcoût par tour (~0,8 ms) est constant alors que la période est dix fois plus courte. Il reste faible car `time.sleep` utilise un timer haute résolution sous Windows depuis Python 3.11 ; il grandirait sur une machine chargée ou avec un intervalle encore plus court. Correction possible : cadencer sur une horloge absolue (`prochain_tour += INTERVAL_S`).

**Mesure 8**

| Observation | Valeur |
|---|---|
| Le processus s'arrête-t-il ? | Non (conteneur resté `Up (healthy)` pendant toute la coupure) |
| Première ligne d'erreur | Une ligne d'information d'abord : `%6\|FAIL\|… kafka:9092/1: Disconnected (after 12110ms in state UP)`. Première erreur : `%3\|FAIL\|… kafka:9092/1: Failed to resolve 'kafka:9092': Name or service not known (after 3318ms in state CONNECT)`. Pas de « Connection refused » pendant la coupure, car le conteneur arrêté disparaît du DNS Docker ; il n'apparaît qu'une fois, juste après `start`, avant que le broker écoute. Pas de « 1/1 brokers are down » observé. |
| Délai stop → première erreur | 0,5 s pour la déconnexion (`%6`), 4,7 s pour la première erreur (`%3`) |
| Reprise après `start` sans relance ? | Oui : broker `healthy` 14 s après `start`, les offsets reprennent leur progression sans redémarrer le producteur |
| Messages perdus pendant la coupure ? | Non : bilan `envoyés=912 non_livrés=0` (arrêt propre par `docker compose stop producer`, signal 15) ; somme finale des offsets 11921 = 1565 + 864 + 8580 + 912 exactement, donc ni perte ni doublon (coupure ~32 s, bien en deçà de `message.timeout.ms`) |

**Question 5.2** — Entre `stop` et `start`, `produce()` continue d'accepter les messages : ils s'accumulent dans le buffer local de librdkafka tandis que le client tente en boucle de se reconnecter. Avec `acks=all`, aucun message n'est considéré livré tant que le broker ne l'a pas écrit, donc rien n'est marqué perdu tant qu'il reste du temps ; au retour du broker, les lots en attente sont réémis (`retries`), et `enable.idempotence=True` garantit, grâce au PID et aux numéros de séquence, qu'aucun doublon ni inversion d'ordre n'apparaît. Les reconnexions elles-mêmes ne consomment pas les `retries` : ce qui borne la tenue sans broker, c'est `message.timeout.ms` (`delivery.timeout.ms`, 300 s par défaut), au-delà duquel le callback reçoit une erreur `Message timed out`. L'autre borne est la taille du buffer (`queue.buffering.max.messages` / `max.kbytes`), dont le dépassement lève un `BufferError` sur `produce()`.

## Nettoyage

**Question 6.1** — `docker compose down` supprime conteneurs et réseau mais conserve les volumes nommés : au prochain `up`, le broker retrouve son répertoire de données, donc le topic `mimic-vitals` et tous ses messages/offsets. `docker compose down -v` supprime aussi les volumes : au prochain `up`, le broker démarre sur un stockage vierge (formaté avec le `CLUSTER_ID`), le topic n'existe plus et doit être recréé — et comme l'auto-création est désactivée, le producteur échouera tant qu'on ne l'a pas fait.
