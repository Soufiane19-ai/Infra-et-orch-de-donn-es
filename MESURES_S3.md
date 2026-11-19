# Mesures — séance 3

> Toutes les valeurs ont été relevées sur le poste le 19 novembre 2026 (Windows 10, Docker Desktop, Git Bash avec `MSYS_NO_PATHCONV=1`), heures en Europe/Paris sauf mention UTC. Les réponses aux questions ont été confrontées aux mesures et ajustées là où elles différaient.

## Prérequis

| Grandeur | Valeur relevée |
|---|---|
| Version de Docker Engine | 28.5.1 (Docker Desktop, Windows 10) |
| Mémoire allouée au moteur Docker (Go) | 7,6 Gio (8 176 435 200 octets, soit 8,2 Go) |
| Mémoire consommée par la pile de la séance 2 seule | ≈ 538 Mio (kafka 420, postgres 95, consumer 13, producer 10) |

| Contrôle | Valeur relevée |
|---|---|
| Nombre de partitions de `mimic-vitals` | 6 (RF 1) |
| Topic recréé au début de cette séance (oui / non) | oui (`down -v` de la séance 2 l'avait supprimé) |
| Nombre de lignes déjà présentes dans `vitals_raw` | 15 012 (12 patients), toutes ingérées par la pile S2 relancée en début de séance (12:15–12:36) ; `vitals_raw` était vide après le `down -v` de S2 |

> **Reprise propre.** Une première tentative d'exécution avait été interrompue sans relevé. Avant les mesures, la base `airflow` a été supprimée (`DROP DATABASE airflow`, `healthstream` intacte), `logs/` et les alertes vidés, pour que les mesures 2, 3, 5 et 8 soient celles d'une première installation.

> **Journaux de tâches sous Windows.** Les journaux s'écrivent bien dans `logs/` malgré les `:` des `run_id` : le partage de fichiers de Docker Desktop traduit chaque `:` en caractère privé U+F03A côté Windows (affiché comme un glyphe inconnu par l'Explorateur et PowerShell), et le reconvertit en `:` vu depuis le conteneur et depuis Git Bash. `ls -R logs/…` et `find … | xargs tail` fonctionnent tels quels ; aucune correction n'a été nécessaire.

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
| Mémoire totale de la pile séance 2 seule | ≈ 538 Mio |
| Mémoire totale de la pile avec Airflow | ≈ 1 740 Mio (1,70 Gio) : web 913, kafka 418, scheduler 329, postgres 58, consumer 12, producer 10 ; Airflow ajoute ≈ 1,2 Gio |
| Conteneur le plus gourmand, et sa consommation | `vitals-airflow-web` : 913 Mio sur 1 Gio de plafond (89 %), au repos, avec un seul worker gunicorn |

**Mesure 2** — coût en temps du démarrage

| Grandeur | Valeur relevée |
|---|---|
| Durée de `airflow-init` (migration + création du compte) | 33 s (11:59:52,6 → 12:00:25,6 UTC, code 0) |
| Délai entre `up -d` et webserver `healthy` | 120 s (`up -d` 11:59:43 UTC → premier contrôle de santé réussi 12:01:43 UTC ; gunicorn à l'écoute à 12:01:33) |

**Question 1.1** — Sur ce poste, partager le serveur PostgreSQL économise un conteneur entier et quelques centaines de Mo de mémoire, le budget le plus serré de la séance, tout en gardant les deux jeux de données étanches dans deux bases distinctes. En production, ce serait une mauvaise idée, car les deux charges et les deux pannes seraient couplées : le scheduler interroge sa base en continu (heartbeats, états des tâches) et concurrence les écritures cliniques. De plus, une maintenance d'Airflow (migration de schéma, restauration, saturation des connexions) arrêterait aussi l'ingestion des signes vitaux. On voudrait enfin des droits, des sauvegardes et une conformité (données de santé) séparés pour des données cliniques et des métadonnées techniques.

**Coupes appliquées (section 1.5)** : seule la coupe 1 (`docker compose stop producer`), appliquée par choix d'architecture et non par manque de mémoire : c'est le DAG qui produit pendant les parties 3 à 5. Aucune coupe mémoire n'a été nécessaire : 1,7 Gio consommés sur 7,6 Gio alloués au moteur, aucun `Exited (137)` ni `OOMKilled`. Point de vigilance : le webserver tourne à 89 % de son plafond de 1 Gio ; c'est lui qui serait tué le premier, et la coupe 4 (le supprimer) rendrait ≈ 900 Mio.

## Partie 2 — Premier DAG

**Mesure 3** — délai de détection

| Grandeur | Valeur relevée |
|---|---|
| Délai entre l'enregistrement de `premier_dag_s3.py` et son apparition | 36 s (enregistré 13:02:26,8, présent dans la table `dag` à 13:03:03,0 ; scrutation toutes les 2 s) — sous le plafond de 60 s fixé par `DAG_DIR_LIST_INTERVAL` |
| Valeur de `AIRFLOW__SCHEDULER__DAG_DIR_LIST_INTERVAL` dans le Compose | 60 s |

**Question 2.1** — `data_interval_start` ne vaut pas la date d'aujourd'hui mais celle d'hier à minuit (Europe/Paris), et `data_interval_end` vaut aujourd'hui à minuit. Avec `schedule="@daily"`, un run ne traite jamais la journée en cours, qui n'est pas terminée : il traite le dernier intervalle complet. Un déclenchement manuel reprend la même règle et s'attache à l'intervalle quotidien qui vient de se clore, ce que confirment les journaux du run `manual__2026-11-19T12:03:31+00:00` : `logical_date = 2026-11-19 12:03:31+00:00`, mais `data_interval_start = 2026-11-17 23:00:00+00:00` et `data_interval_end = 2026-11-18 23:00:00+00:00`, soit `[18 nov 00:00, 19 nov 00:00[` à Paris.

## Partie 3 — Orchestrer la chaîne réelle

**Mesure 4** — la fenêtre d'ingestion

| Grandeur | Valeur relevée |
|---|---|
| `lag_before` (journaux de `relever_offsets_avant`) | 0 (`detail={0: 0, …, 5: 0}`) |
| Lag maximal observé pendant `controler_lag` | 11 (une seule mesure, déjà sous le seuil de 200 : aucune attente) |
| `lag_after` final | 11 (`detail={0: 2, 1: 1, 2: 0, 3: 3, 4: 3, 5: 2}`) |
| `rows_ingested` dans le rapport | 2 268 (12 patients, 0 anomalie) |
| Écart entre `rows_ingested` et 12 × `FENETRE_DUREE_S` (= 720) | +1 548, entièrement dû au service `producer` relancé par `up -d` à 13:00:26 et arrêté à 13:02:20 (1 548 lignes ingérées dans la fenêtre) ; la part produite par le DAG (`produire_fenetre` à 13:04–13:05) vaut exactement 720 lignes (277 + 443), soit un écart nul |

> **Run de mesure (mesures 4, 6 et 7)** : `scheduled__2026-11-19T12:00:00+00:00`, fenêtre `[13:00, 14:00[` Paris. Le premier run (`scheduled__…T11:00:00+00:00`, créé au dégel du DAG à 13:04) a servi de production et contrôlait la fenêtre 12h (20 424 lignes de la pile S2, succès). Une autre session avait remis le DAG en pause à 13:05:54 (`airflow dags pause`, visible dans la table `log`), si bien qu'aucun run n'est parti à 14:00 ; à 14:03, `dags unpause` a créé aussitôt le run de la fenêtre 13h. Aucun `dags trigger` supplémentaire n'a été lancé : il aurait créé un second run sur la même fenêtre et faussé `run_count`.

**Question 3.1** — Avec `subscribe()`, la tâche deviendrait un membre du groupe `vitals-persister` : son arrivée, puis son départ, déclencheraient deux rééquilibrages, et une partie des partitions lui serait attribuée. Ces partitions ne seraient plus écrites en base par le consommateur de la séance 2, et des messages pourraient être acquittés sans jamais être insérés, donc perdus, si la sonde lisait ou commitait.

**Question 3.2** — Si le consommateur est arrêté de 10 h 50 à 11 h 20, un relevé mesuré à 10 h 55 (`event_time`) n'est écrit qu'à 11 h 15 (`ingested_at`) : il compte dans la fenêtre de 10 h selon `event_time`, et dans celle de 11 h selon `ingested_at`. Les deux filtres divergent dès qu'il y a du retard d'ingestion : panne, rattrapage du lag, rejeu de l'historique, horloge du moniteur décalée. Un rapport d'ingestion doit filtrer sur `ingested_at`, parce qu'il mesure ce que la chaîne a absorbé dans l'heure ; `event_time` sert à l'analyse clinique.

## Partie 4 — Robustesse

**Mesure 5** — comportement à l'échec (broker arrêté)

| Observation | Valeur relevée |
|---|---|
| Nombre total d'exécutions de `verifier_broker` avant abandon | 3 (`Starting attempt 1/2/3 of 3`, chacune ≈ 10 s : `list_topics(timeout=10)` puis `_TRANSPORT`) |
| Délai entre la tentative 1 et la tentative 2 | 49 s de début à début (13:05:43 → 13:06:32) ; 39 s entre l'échec et la relance (base 30 s + décalage aléatoire) |
| Délai entre la tentative 2 et la tentative 3 | 88 s de début à début (13:06:32 → 13:08:01) ; 78 s entre l'échec et la relance (base 60 s + décalage aléatoire) |
| État affiché des tâches situées en aval de la tâche échouée | `upstream_failed` (les 6 tâches, `try_number` 0) ; `creer_table_rapport` en `success` |

Run utilisé : `manual__broker_coupe`, déclenché avec `-e 2026-11-19T13:30:00+01:00` (fenêtre 12h), pour ne pas toucher la ligne de la fenêtre 13h, réservée à la mesure 6. Alerte écrite : `data/alertes/alerte_manual__broker_coupe_verifier_broker.json` (tentative 3, `Broker transport failure`, chemin du journal `attempt=3.log`). Reprise 4.3 : `tasks clear … --task-regex verifier_broker --downstream --start-date 2026-11-19T12:29:00+00:00 --end-date 2026-11-19T12:31:00+00:00`, puis Kafka relancé : les 7 tâches repassent et le run finit en `success` (rapport de la fenêtre 12h, `run_count` 2).

**Question 4.1** — Les tâches aval sont en `upstream_failed`, pas en `failed`. `failed` signifie que la tâche a été exécutée et a échoué elle-même, avec un journal à lire. `upstream_failed` signifie qu'elle n'a jamais été lancée parce qu'un prérequis a échoué : il est inutile de chercher sa cause en elle, il faut remonter à la première tâche `failed`, seule porteuse du diagnostic.

**Mesure 6** — idempotence

| Grandeur | Valeur relevée |
|---|---|
| Lignes dans `ingestion_report` pour la fenêtre, après 1 exécution | 1 (`run_count` 1) |
| Lignes pour la même fenêtre, après 2 rejeux | 1 |
| Valeur de `run_count` après les rejeux | 3 (`rows_ingested` inchangé à 2 268, `updated_at` avancé) |
| Fichiers dans `data/rapports/` pour cette heure | 1 (`ingestion_20261119_12.json`, réécrit, `"run_count": 3`) |

Rejeux : `tasks clear healthstream_ingestion --task-regex ecrire_rapport --yes --start-date 2026-11-19T11:59:00+00:00 --end-date 2026-11-19T12:01:00+00:00`, deux fois. (Le nom du fichier porte l'heure UTC du début de fenêtre : `_12` = 13h à Paris.)

**Question 4.2** — C'est une propriété acceptable ici : rejouer `produire_fenetre` crée de *nouveaux* messages, avec de nouveaux offsets, c'est-à-dire de nouvelles mesures simulées, comme un moniteur qui continue de mesurer, et non une copie d'un message déjà émis. La clé primaire `(kafka_partition, kafka_offset)` garantit déjà qu'un même message Kafka, relu après une panne, n'est écrit qu'une fois dans `vitals_raw`, donc que l'ingestion est idempotente. Elle ne fusionne volontairement pas deux messages distincts, car deux relevés légitimes peuvent se ressembler. Dans l'architecture cible, la production n'est de toute façon pas pilotée par Airflow (ce sont les moniteurs qui émettent), et cette tâche disparaît.

## Partie 5 — Mesure

**Mesure 7** — profil de durée d'un run réussi

| Tâche | Durée relevée |
|---|---|
| `verifier_broker` | 0,38 s |
| `produire_fenetre` | 60,67 s |
| `controler_lag` | 0,42 s |
| `verifier_integrite` + `ecrire_rapport` | 0,86 s (0,39 + 0,47) |
| Durée totale du DagRun | 71,04 s (13:03:53,4 → 13:05:04,4 UTC) |

Run `scheduled__2026-11-19T12:00:00+00:00`, durées lues dans `task_instance.duration`. Les 8 tâches cumulent 63,25 s (avec `creer_table_rapport` 0,48 s, `relever_offsets_avant` 0,44 s, `fin_pipeline` 0 s), soit un écart de 7,8 s, ≈ 0,9 s entre la fin d'une tâche et le début de la suivante. Aucun chevauchement : tâches strictement en file. Pic mémoire du scheduler pendant le run : 600 Mio sur un plafond de 1 536 Mio, ce qui confirme une marge confortable.

**Question 5.1** — L'écart n'est pas nul (71,0 s contre 63,3 s, soit 7,8 s) : la durée du DagRun dépasse la somme des durées de tâches. Première cause : la latence d'ordonnancement entre deux tâches, car le scheduler doit constater la fin de l'une, passer la suivante en `scheduled` puis `queued` à son prochain tour de boucle, et le LocalExecutor doit la prendre en charge. Seconde cause : le coût fixe de chaque tâche hors de sa durée mesurée, c'est-à-dire le lancement d'un sous-processus, l'import d'Airflow et du fichier DAG, l'écriture de l'état en base et les callbacks ; huit tâches en série le paient huit fois.

**Mesure 8** — rattrapage

| Grandeur | Valeur relevée |
|---|---|
| Nombre de DagRun créés par le backfill | 3 DagRun traités sur la plage 10h–12h Paris (logical dates 09:00Z, 10:00Z, 11:00Z) : `backfill__2026-11-19T10:00:00+01:00`, `backfill__2026-11-19T10:00:00+00:00`, et le run existant `scheduled__2026-11-19T11:00:00+00:00`, réutilisé et non dupliqué (une seule ligne par date logique) |
| Nombre de lignes ajoutées dans `ingestion_report` | 0 (2 lignes avant, 2 après) : la seule fenêtre réussie (12h, `window_start` 11:00Z) avait déjà sa ligne, mise à jour par l'upsert (`run_count` passé à 4, `rows_ingested` inchangé à 20 424) |
| Nombre de ces runs ayant échoué, et sur quelle tâche | 2 sur 3 (fenêtres 10h et 11h, sans aucune ingestion), tous deux sur `verifier_integrite` (`aucune ligne ingérée dans la fenêtre`, 3 tentatives chacun), aval en `upstream_failed` ; les tâches amont, dont `produire_fenetre`, ont réussi : 720 messages produits *maintenant* par run, pour rien. Alertes : `alerte_backfill__2026-11-19T10-00-00-01-00_verifier_integrite.json` et `…T10-00-00-00-00_…` |

Commande finale : `airflow dags backfill healthstream_ingestion --start-date "2026-11-19T10:00:00+01:00" --end-date "2026-11-19T12:00:00+01:00" --reset-dagruns --continue-on-failures --yes`, DAG en pause (14:59 → 15:15, ≈ 16 min). Ce qu'ont montré les essais précédents :
1. Sans `--continue-on-failures`, le backfill (`max_active_runs=1`, donc un run à la fois) s'arrête au premier run échoué (`BackfillUnfinished`, « finished run 1 of 3 ») : les deux autres fenêtres ne sont jamais traitées.
2. `--reset-dagruns` remet aussi à zéro le run *planifié* déjà présent dans la plage ; le scheduler le relance aussitôt, et un second backfill lancé pendant ce temps refuse (« there's already scheduled in a RUNNING state ») puis se bloque en *deadlock* en convertissant le run en type `backfill`. Le scheduler ignorant les runs `backfill`, ce run resté à moitié fait n'a plus avancé qu'avec un nouveau backfill. Leçon : mettre le DAG en pause avant un backfill qui recouvre des runs planifiés.

**Question 5.2** — Pour la fenêtre de 10 h rattrapée à 14 h, `rows_ingested` compte les lignes dont `ingested_at` tombe entre 10 h et 11 h, c'est-à-dire ce que la chaîne a réellement ingéré ce matin-là : zéro si rien ne tournait, ce qui fait échouer `verifier_integrite`, et jamais les 720 messages produits à 14 h. Ces messages tombent dans la fenêtre de 14 h, faussent son rapport et le lag mesuré pendant le rattrapage : la production du run n'a aucun rapport avec la fenêtre qu'il contrôle. Pour qu'un rattrapage ait un sens, il faut séparer production et contrôle : le producteur doit tourner en continu hors d'Airflow, et le DAG ne fait que mesurer et contrôler une fenêtre passée. À défaut, il faut sauter `produire_fenetre` dès que le run n'est pas le run courant (`ShortCircuitOperator` sur `run_type == "backfill"` ou sur `data_interval_end` nettement antérieur à l'heure réelle).

## Contrôle final et nettoyage

| Contrôle de fin | Valeur relevée |
|---|---|
| Taille du dossier `logs/` après la séance | 1,4 Mio (`data/` : 22 Kio) |
| Nombre de conteneurs encore en vie (`docker ps -q \| wc -l`) | 0 (volumes `kafka-data`, `pg-data`, `vitals-logs` conservés) |

Contrôle 5.3 avant `down` : aucune erreur d'import ; runs en succès : `scheduled__…T12:00`, `scheduled__…T11:00`, `manual__broker_coupe` ; `ingestion_report` : 2 lignes ; `data/rapports/` : 2 rapports JSON ; `data/alertes/` : 3 alertes (broker coupé, 2 fenêtres vides du backfill). `healthstream_ingestion` est laissé **en pause** dans la base `airflow` (mis en pause pour le backfill) : au prochain `up`, `airflow dags unpause healthstream_ingestion` pour reprendre le rapport horaire.

**Question 6.1** — (a) Oui, on retrouve les DAG : ce sont des fichiers de `./dags`, sur l'hôte, simplement montés dans les conteneurs. (b) Oui, on retrouve l'historique des DagRun et des tentatives : il vit dans la base `airflow`, stockée sur le volume `pg-data`, que `down` sans `-v` conserve ; les journaux texte, eux, sont dans `./logs` sur l'hôte. (c) Oui, on retrouve les lignes de `vitals_raw` : base `healthstream`, même volume `pg-data`. (d) Oui, on retrouve le compte admin : il est dans les tables de la base `airflow` (`ab_user`), sur `pg-data`, et au redémarrage `airflow-init` le retrouve et son `users create … || true` ne fait rien. Seul `down -v` effacerait (b), (c) et (d), ainsi que le topic stocké dans `kafka-data`.
