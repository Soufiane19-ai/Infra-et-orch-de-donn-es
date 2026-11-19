# Mesures — séance 3

> Les cases `⟨à relever⟩` sont remplies après exécution sur le poste. Les réponses aux questions sont rédigées à partir de la théorie et de tests locaux (Airflow 2.10.5 + PostgreSQL) ; elles sont ajustées si les mesures diffèrent.

## Prérequis

| Grandeur | Valeur relevée |
|---|---|
| Version de Docker Engine | ⟨à relever⟩ |
| Mémoire allouée au moteur Docker (Go) | ⟨à relever⟩ |
| Mémoire consommée par la pile de la séance 2 seule | ⟨à relever⟩ |

| Contrôle | Valeur relevée |
|---|---|
| Nombre de partitions de `mimic-vitals` | ⟨à relever⟩ |
| Topic recréé au début de cette séance (oui / non) | ⟨à relever⟩ |
| Nombre de lignes déjà présentes dans `vitals_raw` | ⟨à relever⟩ |

### Écarts assumés par rapport à l'énoncé

| Énoncé | Ce projet | Raison |
|---|---|---|
| `/opt/kafka/bin/kafka-topics.sh` (image `apache/kafka`) | `kafka-topics`, `kafka-consumer-groups`, `kafka-get-offsets` (image `cp-kafka:7.8.0` des séances 1-2) | broker conservé, mêmes outils sans `.sh` |
| `AIRFLOW_UID=$(id -u)` | `AIRFLOW_UID=50000` | sous Windows, `id -u` (Git Bash) rend un UID sans signification pour Docker Desktop, qui ignore la propriété des bind mounts ; 50000 est l'UID natif de l'image |
| `alerte_<run_id>_<task_id>.json` | mêmes éléments, `:` et `+` du `run_id` remplacés par `-` | ces caractères sont interdits dans un nom de fichier Windows (`data/` est monté depuis l'hôte) ; le champ `run_id` du JSON garde la valeur exacte |
| Producteur S1 sur `wolfi-base` | producteur S1 sur `python:3.12-slim`, inchangé | `producer.py` n'a besoin que de `confluent-kafka`, installé dans l'image Airflow |

Raccourcis :

```bash
AF="docker compose exec airflow-scheduler airflow"
PSQL="docker compose exec postgres psql -U hs_user -d healthstream"
KT="docker compose exec kafka kafka-topics --bootstrap-server localhost:9092"
KG="docker compose exec kafka kafka-consumer-groups --bootstrap-server localhost:9092"
```

## Décisions retenues

| Décision | Valeur | Justification |
|---|---|---|
| Image de base Airflow | `apache/airflow:2.10.5-python3.12` | dernière 2.x, version figée ; même Python que le producteur et le consommateur |
| Paquet ajouté | `confluent-kafka==2.6.1` | même client que les séances 1 et 2, absent de l'image officielle |
| `AIRFLOW_UID` | `50000` (`.env`) | voir écarts ci-dessus |
| Dossier des DAG | `./dags:/opt/airflow/dags` | seul dossier lu par le scheduler |
| `AIRFLOW__CORE__EXECUTOR` | `LocalExecutor` | parallélisme local suffisant, sans Celery, Redis ni Flower |
| Base de métadonnées | `…@postgres:5432/airflow` | seconde base dans le serveur PostgreSQL de la séance 2 |
| `AIRFLOW__CORE__LOAD_EXAMPLES` | `"false"` | les DAG d'exemple occupent le parseur et l'interface pour rien |
| `AIRFLOW__SCHEDULER__PARSING_PROCESSES` | `"1"` | deux fichiers DAG : un seul processus de parsing suffit et économise la mémoire |
| Dépendance de `airflow-init` | `service_completed_successfully` | attendre la *fin* de la création de la base, pas son démarrage |
| Migration du schéma | `airflow db migrate` | crée ou met à jour les tables de métadonnées, rejouable |
| Plafond mémoire du scheduler | `1536m` | scheduler, plus jusqu'à 4 tâches en sous-processus (≈ 150–200 Mo chacune) ; à confronter à la mesure 1 |
| Port de l'interface | `"8080:8080"` | port libre, identique des deux côtés |
| `schedule` de `premier_dag_s3` | `"@daily"` | une fois par jour |
| `catchup` | `False` | sinon, activer le DAG lancerait un run par jour depuis le 1er septembre |
| `task_id` de la tâche Python | `contexte` | celui qu'utilisent les commandes de lecture des journaux |
| Dépendances de `premier_dag_s3` | `debut >> horodater >> contexte` | trois tâches en file |
| Commande de déclenchement | `airflow dags trigger` | — |
| `get_watermark_offsets(..., cached=…)` | `False` | une borne en cache peut être périmée : un lag faux fait passer ou échouer `controler_lag` à tort |
| Variable de topic | `Variable.get("vitals_topic")` | déclarée par `AIRFLOW_VAR_VITALS_TOPIC` |
| Variable de seuil | `int(Variable.get("lag_max"))` | déclarée par `AIRFLOW_VAR_LAG_MAX` (200) |
| Borne basse de la fenêtre | `context["data_interval_start"]` | l'intervalle du run, jamais l'horloge : la tâche reste rejouable |
| Colonne de filtrage | `ingested_at` | un rapport d'ingestion compte ce que la chaîne a absorbé (Q3.2) |
| Clause de conflit | `ON CONFLICT (dag_id, window_start) DO UPDATE SET …` | le rejeu met à jour la ligne de la fenêtre au lieu d'en créer une seconde |
| Dépendance manquante du DAG | `produire_fenetre >> controler_lag >> verifier_integrite` | — |
| `schedule` de `healthstream_ingestion` | `"@hourly"` | rapport d'ingestion horaire |
| `max_active_runs` | `1` | deux producteurs simultanés fausseraient débit et lag |
| `INTERVAL_S` de `produire_fenetre` | `"1.0"` | même cadence que la séance 1 : 12 msg/s, soit ≈ 720 messages par fenêtre de 60 s, absorbés par le consommateur (lag nominal ≈ 60 en S2, sous le seuil de 200) |
| `retries` | `2` | trois exécutions au plus : couvre un redémarrage du broker sans retarder longtemps le diagnostic d'une erreur déterministe |
| `retry_delay` | `30` s | puis environ 60 s en backoff exponentiel (plus le décalage aléatoire) : laisse au broker le temps de redémarrer (≈ 16 s mesurées en S1) |
| `execution_timeout` | `5` minutes | supérieur à la tâche la plus lente (`controler_lag` ≤ 120 s ; `produire_fenetre` ≈ 60 s) avec une large marge |
| Service `producer` pendant les mesures | arrêté (`docker compose stop producer`) | architecture cible de la séance : c'est le DAG qui produit ; les mesures de lag et de lignes ne mélangent pas deux producteurs |

### Ce que vérifie un run manuel (testé localement)

Un run déclenché à la main traite le **dernier intervalle complet** précédant le déclenchement, pas l'intervalle en cours. Par exemple, `premier_dag_s3` déclenché le 19 novembre traite `[18 nov 00:00, 19 nov 00:00[` (Europe/Paris). De même, `healthstream_ingestion` déclenché à 14 h 10 contrôle `[13:00, 14:00[`. Le premier run d'une nouvelle pile échoue donc en `verifier_integrite` si l'heure précédente n'a rien ingéré : c'est le comportement voulu, celui qui signale une fenêtre trouée.

## Partie 1 — Airflow dans le Compose

**Mesure 1** — coût mémoire

| Grandeur | Valeur relevée |
|---|---|
| Mémoire totale de la pile séance 2 seule | ⟨à relever⟩ |
| Mémoire totale de la pile avec Airflow | ⟨à relever⟩ |
| Conteneur le plus gourmand, et sa consommation | ⟨à relever⟩ |

**Mesure 2** — coût en temps du démarrage

| Grandeur | Valeur relevée |
|---|---|
| Durée de `airflow-init` (migration + création du compte) | ⟨à relever⟩ s |
| Délai entre `up -d` et webserver `healthy` | ⟨à relever⟩ s |

**Question 1.1** — Sur ce poste, partager le serveur PostgreSQL économise un conteneur entier et quelques centaines de Mo de mémoire, le budget le plus serré de la séance, tout en gardant les deux jeux de données étanches dans deux bases distinctes. En production, ce serait une mauvaise idée, car les deux charges et les deux pannes seraient couplées : le scheduler interroge sa base en continu (heartbeats, états des tâches) et concurrence les écritures cliniques. De plus, une maintenance d'Airflow (migration de schéma, restauration, saturation des connexions) arrêterait aussi l'ingestion des signes vitaux. On voudrait enfin des droits, des sauvegardes et une conformité (données de santé) séparés pour des données cliniques et des métadonnées techniques.

**Coupes appliquées (section 1.5)** : ⟨à relever⟩

## Partie 2 — Premier DAG

**Mesure 3** — délai de détection

| Grandeur | Valeur relevée |
|---|---|
| Délai entre l'enregistrement de `premier_dag_s3.py` et son apparition | ⟨à relever⟩ s |
| Valeur de `AIRFLOW__SCHEDULER__DAG_DIR_LIST_INTERVAL` dans le Compose | 60 s |

**Question 2.1** — ⟨à confirmer⟩ `data_interval_start` ne vaut pas la date d'aujourd'hui mais celle d'hier à minuit (Europe/Paris), et `data_interval_end` vaut aujourd'hui à minuit. Avec `schedule="@daily"`, un run ne traite jamais la journée en cours, qui n'est pas terminée : il traite le dernier intervalle complet. Un déclenchement manuel reprend la même règle et s'attache à l'intervalle quotidien qui vient de se clore, ce qui est vérifié localement (déclenché le 19 novembre, il traite `[18 nov 00:00, 19 nov 00:00[`).

## Partie 3 — Orchestrer la chaîne réelle

**Mesure 4** — la fenêtre d'ingestion

| Grandeur | Valeur relevée |
|---|---|
| `lag_before` (journaux de `relever_offsets_avant`) | ⟨à relever⟩ |
| Lag maximal observé pendant `controler_lag` | ⟨à relever⟩ |
| `lag_after` final | ⟨à relever⟩ |
| `rows_ingested` dans le rapport | ⟨à relever⟩ |
| Écart entre `rows_ingested` et 12 × `FENETRE_DUREE_S` (= 720) | ⟨à relever⟩ |

**Question 3.1** — Avec `subscribe()`, la tâche deviendrait un membre du groupe `vitals-persister` : son arrivée, puis son départ, déclencheraient deux rééquilibrages, et une partie des partitions lui serait attribuée. Ces partitions ne seraient plus écrites en base par le consommateur de la séance 2, et des messages pourraient être acquittés sans jamais être insérés, donc perdus, si la sonde lisait ou commitait.

**Question 3.2** — Si le consommateur est arrêté de 10 h 50 à 11 h 20, un relevé mesuré à 10 h 55 (`event_time`) n'est écrit qu'à 11 h 15 (`ingested_at`) : il compte dans la fenêtre de 10 h selon `event_time`, et dans celle de 11 h selon `ingested_at`. Les deux filtres divergent dès qu'il y a du retard d'ingestion : panne, rattrapage du lag, rejeu de l'historique, horloge du moniteur décalée. Un rapport d'ingestion doit filtrer sur `ingested_at`, parce qu'il mesure ce que la chaîne a absorbé dans l'heure ; `event_time` sert à l'analyse clinique.

## Partie 4 — Robustesse

**Mesure 5** — comportement à l'échec (broker arrêté)

| Observation | Valeur relevée |
|---|---|
| Nombre total d'exécutions de `verifier_broker` avant abandon | ⟨à relever⟩ |
| Délai entre la tentative 1 et la tentative 2 | ⟨à relever⟩ s |
| Délai entre la tentative 2 et la tentative 3 | ⟨à relever⟩ s |
| État affiché des tâches situées en aval de la tâche échouée | ⟨à relever⟩ |

**Question 4.1** — Les tâches aval sont en `upstream_failed`, pas en `failed`. `failed` signifie que la tâche a été exécutée et a échoué elle-même, avec un journal à lire. `upstream_failed` signifie qu'elle n'a jamais été lancée parce qu'un prérequis a échoué : il est inutile de chercher sa cause en elle, il faut remonter à la première tâche `failed`, seule porteuse du diagnostic.

**Mesure 6** — idempotence

| Grandeur | Valeur relevée |
|---|---|
| Lignes dans `ingestion_report` pour la fenêtre, après 1 exécution | ⟨à relever⟩ |
| Lignes pour la même fenêtre, après 2 rejeux | ⟨à relever⟩ |
| Valeur de `run_count` après les rejeux | ⟨à relever⟩ |
| Fichiers dans `data/rapports/` pour cette heure | ⟨à relever⟩ |

**Question 4.2** — C'est une propriété acceptable ici : rejouer `produire_fenetre` crée de *nouveaux* messages, avec de nouveaux offsets, c'est-à-dire de nouvelles mesures simulées, comme un moniteur qui continue de mesurer, et non une copie d'un message déjà émis. La clé primaire `(kafka_partition, kafka_offset)` garantit déjà qu'un même message Kafka, relu après une panne, n'est écrit qu'une fois dans `vitals_raw`, donc que l'ingestion est idempotente. Elle ne fusionne volontairement pas deux messages distincts, car deux relevés légitimes peuvent se ressembler. Dans l'architecture cible, la production n'est de toute façon pas pilotée par Airflow (ce sont les moniteurs qui émettent), et cette tâche disparaît.

## Partie 5 — Mesure

**Mesure 7** — profil de durée d'un run réussi

| Tâche | Durée relevée |
|---|---|
| `verifier_broker` | ⟨à relever⟩ s |
| `produire_fenetre` | ⟨à relever⟩ s |
| `controler_lag` | ⟨à relever⟩ s |
| `verifier_integrite` + `ecrire_rapport` | ⟨à relever⟩ s |
| Durée totale du DagRun | ⟨à relever⟩ s |

**Question 5.1** — ⟨à confirmer⟩ L'écart n'est pas nul : la durée du DagRun dépasse la somme des durées de tâches. Première cause : la latence d'ordonnancement entre deux tâches, car le scheduler doit constater la fin de l'une, passer la suivante en `scheduled` puis `queued` à son prochain tour de boucle, et le LocalExecutor doit la prendre en charge. Seconde cause : le coût fixe de chaque tâche hors de sa durée mesurée, c'est-à-dire le lancement d'un sous-processus, l'import d'Airflow et du fichier DAG, l'écriture de l'état en base et les callbacks ; huit tâches en série le paient huit fois.

**Mesure 8** — rattrapage

| Grandeur | Valeur relevée |
|---|---|
| Nombre de DagRun créés par le backfill | ⟨à relever⟩ |
| Nombre de lignes ajoutées dans `ingestion_report` | ⟨à relever⟩ |
| Nombre de ces runs ayant échoué, et sur quelle tâche | ⟨à relever⟩ |

**Question 5.2** — Pour la fenêtre de 10 h rattrapée à 14 h, `rows_ingested` compte les lignes dont `ingested_at` tombe entre 10 h et 11 h, c'est-à-dire ce que la chaîne a réellement ingéré ce matin-là : zéro si rien ne tournait, ce qui fait échouer `verifier_integrite`, et jamais les 720 messages produits à 14 h. Ces messages tombent dans la fenêtre de 14 h, faussent son rapport et le lag mesuré pendant le rattrapage : la production du run n'a aucun rapport avec la fenêtre qu'il contrôle. Pour qu'un rattrapage ait un sens, il faut séparer production et contrôle : le producteur doit tourner en continu hors d'Airflow, et le DAG ne fait que mesurer et contrôler une fenêtre passée. À défaut, il faut sauter `produire_fenetre` dès que le run n'est pas le run courant (`ShortCircuitOperator` sur `run_type == "backfill"` ou sur `data_interval_end` nettement antérieur à l'heure réelle).

## Contrôle final et nettoyage

| Contrôle de fin | Valeur relevée |
|---|---|
| Taille du dossier `logs/` après la séance | ⟨à relever⟩ |
| Nombre de conteneurs encore en vie (`docker ps -q \| wc -l`) | ⟨à relever⟩ |

**Question 6.1** — (a) Oui, on retrouve les DAG : ce sont des fichiers de `./dags`, sur l'hôte, simplement montés dans les conteneurs. (b) Oui, on retrouve l'historique des DagRun et des tentatives : il vit dans la base `airflow`, stockée sur le volume `pg-data`, que `down` sans `-v` conserve ; les journaux texte, eux, sont dans `./logs` sur l'hôte. (c) Oui, on retrouve les lignes de `vitals_raw` : base `healthstream`, même volume `pg-data`. (d) Oui, on retrouve le compte admin : il est dans les tables de la base `airflow` (`ab_user`), sur `pg-data`, et au redémarrage `airflow-init` le retrouve et son `users create … || true` ne fait rien. Seul `down -v` effacerait (b), (c) et (d), ainsi que le topic stocké dans `kafka-data`.
