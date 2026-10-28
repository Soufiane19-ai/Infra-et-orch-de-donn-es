# Mesures — séance 2

> Les cases `⟨à relever⟩` sont remplies après exécution sur le poste. Les réponses aux questions sont rédigées à partir de la théorie et de la répartition prédite par le hachage murmur2 ; elles sont ajustées si les mesures diffèrent.

## Prérequis

| Contrôle | Valeur relevée |
|---|---|
| Sortie de `docker compose config --quiet` | ⟨à relever⟩ |
| Le volume `kafka-data` existe-t-il encore ? | ⟨à relever⟩ |
| PostgreSQL installé nativement (`psql --version`) | ⟨à relever⟩ |

### Écarts assumés par rapport à l'énoncé

| Énoncé | Ce projet | Raison |
|---|---|---|
| Image Kafka `apache/kafka`, outils `/opt/kafka/bin/*.sh` | `confluentinc/cp-kafka:7.8.0` (séance 1), outils `kafka-topics`, `kafka-consumer-groups`, `kafka-get-offsets` sans `.sh` | broker de la séance 1 conservé ; mêmes outils, autre chemin |
| `--formatter-property print.key=true` | `--property print.key=true` | syntaxe du console-consumer de Kafka 3.8 (cp-kafka 7.8) |
| Producteur S1 sur `wolfi-base` | producteur S1 sur `python:3.12-slim` (inchangé) | le consommateur suit l'énoncé (`wolfi-base` figé par digest) ; il ne peut donc pas partager le digest du producteur |
| Volume `kafka-data` | renommé depuis `vitals-kafka-data` (S1) | contrat d'interface de la séance 3 |

Raccourcis utilisés :

```bash
TOPIC=mimic-vitals
KT="docker compose exec kafka kafka-topics --bootstrap-server localhost:9092"
KG="docker compose exec kafka kafka-consumer-groups --bootstrap-server localhost:9092"
KO="docker compose exec kafka kafka-get-offsets --bootstrap-server localhost:9092"
PSQL="docker compose exec postgres psql -U hs_user -d healthstream"
```

## Décisions retenues

| Décision | Valeur | Justification |
|---|---|---|
| `NB_PARTITIONS` | `6` | multiple de 2 et de 3 : 2 puis 3 consommateurs reçoivent chacun le même nombre de partitions, aucun n'est inactif |
| `auto.offset.reset` du lecteur | `OFFSET_RESET` (env, défaut `earliest`) | configurable pour comparer `earliest` / `latest` sans toucher au code |
| Stratégie du lecteur | `PARTITION_STRATEGY` (env, défaut `range`) | `cooperative-sticky` en 3.4 sans modifier le code |
| Clé primaire | `(kafka_partition, kafka_offset)` | identifie un message de façon unique et stable |
| Clause de conflit | `ON CONFLICT ON CONSTRAINT pk_vitals_raw DO NOTHING` | un rejeu est absorbé silencieusement, la première écriture fait foi |
| `enable.auto.commit` (consumer.py) | `False` | acquittement manuel, après l'écriture en base |
| Stratégie (consumer.py) | `cooperative-sticky` | un rééquilibrage ne retire que les partitions qui changent de main |
| `BATCH_SIZE` / `BATCH_TIMEOUT_S` | `200` / `5.0` | valeurs imposées ; voir Q4.3 |
| Écriture avant retrait | `on_revoke` écrit et acquitte le lot en cours | les partitions rendues repartent d'un offset exact, sans relecture inutile |

### Répartition prédite (murmur2 positif modulo N, partitionneur `murmur2_random`)

| N | p0 | p1 | p2 | p3 | p4 | p5 |
|---|---|---|---|---|---|---|
| 3 | P003 P005 P008 P009 P010 P012 | P002 P006 P007 P011 | P001 P004 | — | — | — |
| 6 | P005 P010 P012 | P007 | *(aucun)* | P003 P008 P009 | P002 P006 P011 | P001 P004 |

Cohérent avec la mesure 5 de la séance 1 (P001 → 2, P002 → 1, P003 → 0).

## Partie 1 — Partitions

**Mesure 1** — état de départ

| Grandeur | Valeur relevée |
|---|---|
| Partitions de `mimic-vitals` avant modification | ⟨à relever⟩ |
| Somme des offsets de fin | ⟨à relever⟩ |

**Mesure 2** — répartition avant modification (3 partitions)

| Partition | Patients observés | Nombre |
|---|---|---|
| 0 | ⟨à relever⟩ | ⟨à relever⟩ |
| 1 | ⟨à relever⟩ | ⟨à relever⟩ |
| 2 | ⟨à relever⟩ | ⟨à relever⟩ |

**Mesure 3** — répartition après passage à 6 partitions (messages récents)

| Partition | Patients observés | Nombre |
|---|---|---|
| 0 | ⟨à relever⟩ | ⟨à relever⟩ |
| 1 | ⟨à relever⟩ | ⟨à relever⟩ |
| 2 | ⟨à relever⟩ | ⟨à relever⟩ |
| 3 | ⟨à relever⟩ | ⟨à relever⟩ |
| 4 | ⟨à relever⟩ | ⟨à relever⟩ |
| 5 | ⟨à relever⟩ | ⟨à relever⟩ |

**Question 1.1** — Non : seuls P005, P010, P012 (partition 0) et P007 (partition 1) sont restés en place, les huit autres patients ont changé de partition. La partition vaut `murmur2(clé) mod N` : passer de N = 3 à N = 6 change le modulo, et comme 6 = 2 × 3, une clé qui était en `p` reste en `p` ou part en `p + 3` (P001 passe de 2 à 5).

**Question 1.2** — Non, la répartition est 3 / 1 / 0 / 3 / 3 / 2 : la partition 2 ne reçoit aucun patient. Douze clés hachées sur six partitions se comportent comme douze tirages aléatoires indépendants, et la probabilité d'obtenir exactement deux clés par partition est très faible ; trous et partitions chargées sont la règle sur un si petit nombre de clés. En partie 3, les consommateurs recevront le même nombre de partitions mais pas le même nombre de patients : le débit sera déséquilibré, et une instance pourra tenir une partition vide.

**Question 1.3** — L'ordre chronologique par patient de la séance 1 est cassé : les anciens messages de P001 sont en partition 2, les nouveaux en partition 5, et un consommateur peut lire un nouveau relevé avant un ancien. La fenêtre dure tant que des messages écrits avant l'`--alter` restent à consommer dans l'ancienne partition : quelques secondes pour un consommateur à jour, mais toute la durée de rétention pour un consommateur en retard ou qui rejoue l'historique.

## Partie 2 — Offsets et acquittement

**Mesure 4**

| Groupe | `auto.offset.reset` | Lus au 1er lancement (20 s) | Lus au 2e lancement (20 s) |
|---|---|---|---|
| `explo-earliest` | `earliest` | ⟨à relever⟩ | ⟨à relever⟩ |
| `explo-latest` | `latest` | ⟨à relever⟩ | ⟨à relever⟩ |

**Mesure 5** — `--describe` de `explo-earliest` après arrêt propre

| Colonne | Valeur relevée (partition 0) |
|---|---|
| CURRENT-OFFSET | ⟨à relever⟩ |
| LOG-END-OFFSET | ⟨à relever⟩ |
| LAG | ⟨à relever⟩ |
| CONSUMER-ID | ⟨à relever⟩ |

**Question 2.1** — Au premier arrêt, le groupe a acquitté sa position dans `__consumer_offsets`, et au second lancement il reprend depuis cet offset commité au lieu de relire. `auto.offset.reset` ne sert que lorsqu'un groupe n'a aucun offset commité sur une partition : il n'a donc joué qu'au tout premier lancement du groupe.

**Question 2.2** — Première façon : lancer le nouveau traitement sous un `group.id` neuf avec `auto.offset.reset=earliest`, qui relit tout sans toucher aux offsets du groupe existant. Seconde façon : réinitialiser les offsets d'un groupe dédié, groupe arrêté, avec `kafka-consumer-groups --reset-offsets --to-earliest --execute` (ou un `seek()` dans le code). En production je choisirais le groupe neuf : il ne demande aucune manipulation des offsets, il est réversible et il laisse tourner le traitement en place pendant que la nouvelle version rattrape.

**Mesure 6**

| Groupe | Mode | Lus avant le kill | CURRENT-OFFSET total après le kill | Relus au redémarrage |
|---|---|---|---|---|
| `explo-auto` | automatique | ⟨à relever⟩ | ⟨à relever⟩ | ⟨à relever⟩ |
| `explo-manuel` | manuel (tous les 100) | ⟨à relever⟩ | ⟨à relever⟩ | ⟨à relever⟩ |

**Question 2.3** — En acquittement automatique, le client commite périodiquement ce que `poll()` a remis à l'application, que le traitement ait réussi ou non. Une panne entre ce commit et la fin du traitement **perd** donc le message : l'acquittement a précédé le traitement. En acquittement manuel placé après le traitement, une panne entre le traitement et le commit fait relire et **traiter deux fois** les messages : l'acquittement suit le traitement. Le second risque est le seul acceptable, à condition de rendre l'écriture idempotente, et c'est ce que construit la partie 4.

## Partie 3 — Consumer groups

**Mesure 7** — attribution (`range`, 6 partitions)

| Situation | cA | cB | cC |
|---|---|---|---|
| cA seul | ⟨à relever⟩ | — | — |
| cA + cB | ⟨à relever⟩ | ⟨à relever⟩ | — |
| cA + cB + cC | ⟨à relever⟩ | ⟨à relever⟩ | ⟨à relever⟩ |

**Mesure 8** — débit par instance pendant 60 s à trois

| Instance | Messages lus (différence des `lus=`) | Patients couverts |
|---|---|---|
| cA | ⟨à relever⟩ | ⟨à relever⟩ |
| cB | ⟨à relever⟩ | ⟨à relever⟩ |
| cC | ⟨à relever⟩ | ⟨à relever⟩ |

**Question 3.1** — Non : chaque instance reçoit deux partitions, mais ces paires couvrent respectivement 4, 3 et 5 patients ({0,1}, {2,3}, {4,5} d'après la mesure 3), donc environ 240, 180 et 300 messages en 60 s. Le déséquilibre du débit reproduit exactement le déséquilibre du hachage observé en mesure 3 : `range` équilibre les partitions, pas les clés.

**Mesure 9**

| Grandeur | Valeur relevée |
|---|---|
| Partitions du topic | 6 |
| Instances lancées | ⟨à relever⟩ |
| Instances affichant `partitions=[aucune]` | ⟨à relever⟩ |
| `lus=` de ces instances après 30 s | ⟨à relever⟩ |

**Question 3.2** — Le parallélisme de consommation d'un groupe est borné par le nombre de partitions : chaque partition n'a qu'un seul propriétaire, et les membres en surnombre restent inactifs. Pour dépasser cette borne, il faut augmenter le nombre de partitions du topic. Comme en question 1.3, les données déjà écrites ne bougent pas, mais les nouvelles clés sont redistribuées, ce qui casse l'ordre par clé pendant la transition.

**Mesure 10**

| Scénario | Délai avant réattribution à cA | `session.timeout.ms` par défaut |
|---|---|---|
| A — `Ctrl-C` (arrêt propre, `close()`) | ⟨à relever⟩ s | 45 000 ms |
| B — `kill -9` (arrêt brutal) | ⟨à relever⟩ s | idem |

**Question 3.3** — En arrêt propre, `close()` envoie un `LeaveGroup` au coordinateur, qui déclenche le rééquilibrage immédiatement. Un processus tué n'annonce rien : le coordinateur ne constate son absence qu'en ne recevant plus de heartbeats pendant `session.timeout.ms`, soit environ 45 s. Mettre 1 s ferait déclarer morts des consommateurs simplement ralentis (pause GC, écriture d'un lot, latence réseau, heartbeat toutes les 3 s par défaut) : on obtiendrait des rééquilibrages en rafale, chacun interrompant la consommation et provoquant des relectures.

**Mesure 11**

| Stratégie | Lignes `RETRAIT` de cA au départ de cB | Partitions listées dans ce `RETRAIT` |
|---|---|---|
| `range` | ⟨à relever⟩ | ⟨à relever⟩ |
| `cooperative-sticky` | ⟨à relever⟩ | ⟨à relever⟩ |

**Question 3.4** — Avec la stratégie coopérative, un consommateur ne rend que les partitions qui changent réellement de main, et continue d'écrire en base pour celles qu'il garde au lieu de tout suspendre. Il n'a donc à écrire et acquitter que le lot des partitions retirées, pas à vider tout son état à chaque arrivée ou départ d'un membre. **Stratégie retenue dans le code de rendu** : `cooperative-sticky` dans `consumer/consumer.py` ; `range` par défaut dans `consumer/lecteur.py`, surchargeable par `PARTITION_STRATEGY`.

## Partie 4 — PostgreSQL

**Question 4.1** — Le couple (partition, offset) est unique par construction : chaque message Kafka en possède exactement un, et le rejouer redonne le même couple, ce qui fait de la clé primaire un détecteur de rejeu parfait. Le triplet `(patient_id, event_time, vital_type)` n'est pas unique par nature : deux relevés légitimes peuvent le partager. Par exemple, la fréquence cardiaque de P007 mesurée à la même milliseconde par l'ECG et par l'oxymètre de pouls : le second relevé serait silencieusement jeté comme « doublon ».

**Mesure 12**

| Grandeur | Valeur relevée |
|---|---|
| Délai entre `up -d postgres` et `healthy` | ⟨à relever⟩ s |
| Nombre de tables listées par `\dt` | ⟨à relever⟩ |
| Contrainte de clé primaire (`\d vitals_raw`) | `pk_vitals_raw` |

**Mesure 13** — consommateur lancé depuis l'hôte, 60 s

| Grandeur | Valeur relevée |
|---|---|
| `lus=` à l'arrêt | ⟨à relever⟩ |
| `inseres=` à l'arrêt | ⟨à relever⟩ |
| `lignes_totales` | ⟨à relever⟩ |
| `messages_distincts` | ⟨à relever⟩ |
| `patients_distincts` | ⟨à relever⟩ |

**Question 4.2** — ⟨à confirmer⟩ Oui, `lignes_totales` = `messages_distincts` : chaque message Kafka n'a donné qu'une ligne. S'ils différaient, un même message aurait été écrit plusieurs fois, ce qui voudrait dire que la clé primaire ne porte pas sur les coordonnées Kafka (ou n'existe pas) et que l'idempotence est cassée.

**Mesure 14** — chaîne complète

| Grandeur | Valeur relevée |
|---|---|
| Services affichés par `docker compose ps` | 4 (`kafka`, `postgres`, `producer`, `consumer`) |
| Services `healthy` | ⟨à relever⟩ |
| Taille moyenne des lots (`LOT taille=`) | ⟨à relever⟩ |
| Croissance de `lignes_totales` sur 60 s | ⟨à relever⟩ lignes |

**Question 4.3** — Le producteur publie environ 12 msg/s (11,86 msg/s mesurés en séance 1), donc atteindre `BATCH_SIZE=200` prendrait 200 / 12 ≈ 17 s. Le délai de 5 s expire bien avant : c'est lui qui déclenche chaque lot, d'environ 12 × 5 ≈ 60 messages. La condition de taille ne sert qu'en rattrapage (redémarrage, lag accumulé), quand des centaines de messages attendent déjà.

## Partie 5 — Résilience

**Mesure 15** — lag nominal

| Instant | Somme des LAG |
|---|---|
| t0 | ⟨à relever⟩ |
| t0 + 30 s | ⟨à relever⟩ |
| t0 + 60 s | ⟨à relever⟩ |

**Question 5.1** — ⟨à confirmer⟩ Le lag est stable : il oscille en dents de scie sans tendance. À l'équilibre, il vaut au plus ce qui s'accumule entre deux acquittements, soit 12 msg/s × 5 s ≈ 60 messages, et en moyenne une trentaine.

**Mesure 16** — lag en surcharge (`INTERVAL_S=⟨à relever⟩`, `BATCH_SIZE=1`, `--cpus 0.05`)

| Instant | Somme des LAG | Écart |
|---|---|---|
| t0 | ⟨à relever⟩ | — |
| t0 + 30 s | ⟨à relever⟩ | ⟨à relever⟩ |
| t0 + 60 s | ⟨à relever⟩ | ⟨à relever⟩ |
| t0 + 120 s | ⟨à relever⟩ | ⟨à relever⟩ |

**Mesure 17** — passage à deux consommateurs

| Grandeur | Valeur relevée |
|---|---|
| Somme des LAG juste avant le `--scale` | ⟨à relever⟩ |
| Somme des LAG 60 s après | ⟨à relever⟩ |
| Membres du groupe (`--describe`) | 2 |
| Le lag décroît / se stabilise / croît ? | ⟨à relever⟩ |

**Question 5.2** — Dans l'ordre, j'examinerais : (1) le coût de traitement par message côté consommateur (ici un INSERT et un acquittement par message, et un CPU bridé) ; (2) le parallélisme réel, c'est-à-dire si les partitions chargées sont bien réparties entre les membres ou si une partition chaude concentre le flux ; (3) la capacité du stockage commun, PostgreSQL, qui reçoit les écritures de tous les consommateurs. La troisième cause ne se corrige pas en ajoutant des consommateurs : ils écriraient tous dans la même base saturée, et le lag continuerait de croître. On ne dépasse pas non plus le nombre de partitions, ni la charge d'une clé chaude qui reste sur une seule partition.

**Mesure 18** — PostgreSQL arrêté 90 s

| Observation | Valeur relevée |
|---|---|
| Le conteneur consommateur s'arrête-t-il ou redémarre-t-il ? | ⟨à relever⟩ |
| Première ligne d'erreur (début) | ⟨à relever⟩ |
| Somme des LAG après 90 s de coupure | ⟨à relever⟩ |
| `lignes_totales` avant la coupure | ⟨à relever⟩ |
| `lignes_totales` 60 s après le redémarrage | ⟨à relever⟩ |
| Somme des LAG 60 s après le redémarrage | ⟨à relever⟩ |
| Rattrapage sans intervention ? | ⟨à relever⟩ |

**Question 5.3** — ⟨à confirmer⟩ Non, les offsets du groupe n'ont pas avancé pendant la coupure : `commit()` n'est appelé qu'après un `inserer_lot()` réussi, donc tant que l'écriture échoue le lot reste en mémoire, rien n'est acquitté et le lag grossit. Au retour de la base, le lot conservé est écrit puis acquitté, et le lag se résorbe. Avec l'ordre inverse, les offsets auraient avancé sur des messages jamais écrits : au redémarrage, la lecture aurait repris après eux et ils auraient été perdus définitivement, laissant un trou silencieux dans `vitals_raw`.

**Mesure 19** — reprise après `SIGKILL`

| Grandeur | Valeur relevée |
|---|---|
| `lignes_totales` avant le SIGKILL | ⟨à relever⟩ |
| `inseres=` de l'instance relancée (ligne `Arret.`) | ⟨à relever⟩ |
| `ignores_doublons=` de l'instance relancée | ⟨à relever⟩ |
| Lignes renvoyées par la requête de doublons | ⟨à relever⟩ |
| Partitions où `lignes ≠ etendue` | ⟨à relever⟩ |

**Question 5.4** — ⟨à adapter⟩ Une valeur non nulle vient de messages déjà écrits en base mais pas encore acquittés au moment du `SIGKILL` (tué entre `inserer_lot()` et `commit()`). L'instance relancée repart du dernier offset commité, relit ces messages, et la clé primaire les rejette. Ces doublons interceptés prouvent que la relecture at-least-once a bien eu lieu et que l'idempotence l'a absorbée. Une valeur nulle signifie que le kill est tombé pendant l'accumulation d'un lot, avant toute écriture : les messages relus n'étaient pas encore en base, ils ont simplement été insérés.

**Question 5.5** — Sur le flux, la chaîne garantit *at-least-once* : l'acquittement suit l'écriture, donc un message peut être relu après une panne mais jamais sauté. Dans la table, l'état final est *exactly-once* : la clé primaire sur (partition, offset) absorbe toute relecture, et chaque message apparaît une et une seule fois. Ce n'est pas l'*exactly-once* transactionnel de Kafka, c'est un *exactly-once* effectif, obtenu par at-least-once plus une écriture idempotente.

## Nettoyage

**Question 6.1** — `docker compose down -v` supprime le volume `kafka-data`, donc le topic, ses messages et `__consumer_offsets` avec les offsets commités de `vitals-persister`, ainsi que le volume `pg-data`, donc toutes les lignes de `vitals_raw`. Au prochain `up`, PostgreSQL repart d'un répertoire vide et rejoue `init.sql`, mais le topic n'existe plus et doit être recréé, puisque l'auto-création est désactivée. Une fois le topic recréé, le consommateur, qui n'a plus aucun offset commité, applique `AUTO_OFFSET_RESET=earliest` et lit depuis l'offset 0 du nouveau topic, dans une table vide : l'état reste cohérent, sans doublon ni perte par rapport à ce qui existe.
