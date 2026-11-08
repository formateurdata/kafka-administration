# Tests du lab

Ce document liste ce qui a été réellement exécuté sur le lab, avec le résultat obtenu, et ce qui ne l'a pas été.
Les résultats attendus des TP et des corrigés proviennent de ces exécutions.

## 1. Environnement de test

| Élément | Valeur |
|---|---|
| Dates | 27 et 28/10/2026 |
| Machine | Apple M1 Max, 10 cœurs, 32 Go de RAM |
| Système | macOS 27.2 |
| Docker | Docker Desktop, Engine 29.9.2 (`linux/arm64`), Compose 5.6.1, magasin d'images containerd |
| Ressources allouées à Docker | 10 CPU, 16 Go |
| Client sur l'hôte (démo D2) | OpenJDK 21.0.5, tarball `kafka_2.13-4.3.2.tgz` (SHA-512 vérifié) |

## 2. Cycle complet depuis un état vierge

Séquence exécutée : `reset.sh --yes`, `start.sh core`, `validate.sh`, `start.sh extended`, `start.sh full`, `validate.sh`, `selftest.sh`.

| Étape | Résultat | Durée |
|---|---|---|
| `reset.sh --yes` | Conteneurs et volumes supprimés, configurations d'origine | 9 s |
| `start.sh core` (images présentes) | 3 nœuds sains | 7 s |
| `validate.sh` (core) | Environnement valide | 19 s |
| `start.sh extended` | Tous les services démarrés | 21 s |
| `start.sh full` | Échec au premier essai (healthcheck de Schema Registry), corrigé puis réussi : voir section 6 | 7 s après correction |
| `validate.sh` (full) | Environnement valide | 26 s |
| `selftest.sh` (full) | 97 vérifications réussies, 0 échec (28/10, après la correction de l'attente de rétention), puis de nouveau 97 sur 97 après la modification de `reset.sh` (sauvegarde de l'incident 5) | 8 min 17 s, puis 6 min 16 s |

## 3. Mesures de ressources

Relevées avec `docker stats` après une minute de fonctionnement.

| Service | core | extended | full |
|---|---|---|---|
| kafka-1, kafka-2, kafka-3 (chacun) | 360 à 380 Mo | 790 à 830 Mo | 830 à 855 Mo |
| kafka-ui | 343 Mo | 360 Mo | 368 Mo |
| grafana | | 432 Mo | 434 Mo |
| prometheus | | 99 Mo | 114 Mo |
| loadgen-producer, loadgen-consumer (chacun) | | 152 à 154 Mo | 154 Mo |
| kafka-exporter | | 16 Mo | 22 Mo |
| schema-registry | | | 332 Mo |
| connect | | | 389 Mo |
| **Total** | **1,5 Go** | **4,1 Go** | **4,9 Go** |

Le service `jupyter`, ajouté le 28/10 après ces mesures, s'y ajoute dans les trois modes : 70 Mo au repos, 195 Mo au maximum pendant l'exécution du corrigé du TP1 (`docker stats`, une mesure par seconde). Image `kafka-lab/jupyter:1` : 634 Mo, construite en 80 s avec Internet.

- La mémoire d'un nœud Kafka sous charge se décompose en 660 à 700 Mo de mémoire de processus et 130 Mo de cache de fichiers (`memory.stat` du cgroup). Aucun événement OOM n'a été relevé (`memory.events`).
- Images, pour l'architecture `arm64` : 5 Go sur disque, 1,6 Go téléchargés. Le lot hors ligne fait 1,6 Go par architecture.

## 4. Fonctions testées

### Cluster et TP1 à TP7 (`selftest.sh`)

| Domaine | Vérification | Résultat |
|---|---|---|
| KRaft | Leader élu, 3 votants, 1 Leader et 2 Follower dans `describe --replication` | OK |
| KRaft | `kafka-features` : `metadata.version` 4.3-IV0, `kraft.version` 1 | OK |
| Topics | Création, ajout de partitions, configuration, suppression ; pas de création automatique | OK |
| Messages | Production avec clé (`orders.txt`), même clé dans la même partition | OK |
| Groupes | Lag, `--reset-offsets` (dry-run, `--to-earliest`, `--shift-by`) | OK |
| Rétention | `retention.ms=60000`, 101e message envoyé au moins 35 s après les 100 premiers : premier offset passé à 100 (le 28/10, 35 s d'attente après le 101e message) | OK |
| Compaction | 21 messages ramenés à 5 (une valeur par clé), 6 s après le roulement de segment | OK |
| Fiabilité | `min.insync.replicas` au niveau broker refusé (ELR) ; au niveau cluster accepté | OK |
| Panne | Arrêt de kafka-3 : URP, `acks=all` accepté avec min ISR 2, refusé (`NOT_ENOUGH_REPLICAS`) avec min ISR 3, `acks=1` accepté, colonne `Elr: 3` | OK |
| Sécurité | SCRAM-SHA-512, SASL_SSL, refus sans ACL (`TopicAuthorization`), ACL Write et Read, refus de groupe (`GroupAuthorization`), refus tracé dans `kafka-authorizer.log` | OK |
| Quotas | `producer_byte_rate=102400` : débit bridé à 0,31 Mo/s (3 brokers), throttle moyen 119 ms | OK |
| Traces | `broker-loggers` : `org.apache.kafka.controller=DEBUG` puis retour au défaut | OK |
| Métriques | Endpoint JMX, contrôleur actif, URP, débit, lag (kafka-exporter), métriques client, 9 règles d'alerte chargées | OK |
| Exploitation | kafka-4 en Observer, réassignation avec throttle, `--verify` | OK |
| Exploitation | Rolling restart sous `acks=all` (contrôleur actif en dernier) : 30 000 envois confirmés, aucun échec, 6 à 14 avertissements retriables, 37 à 40 s | OK |
| Exploitation | Ajout de `log.dirs` (`data2`), déplacement d'une réplique vers `data2` puis retour | OK |
| Exploitation | Décommissionnement : réassignation hors de kafka-4, `kafka-cluster.sh unregister` | OK |

### MirrorMaker 2 et cluster cloud, TP9 (04/11/2026)

Cycle exécuté : `reset.sh --yes`, `start.sh full`, `validate.sh`, `selftest.sh` (tp1 à tp7 et tp9) : 109 vérifications réussies sur 109 en 547 s, dont 12 pour le TP9. Lab remis à l'état vierge ensuite.

| Domaine | Vérification | Résultat |
|---|---|---|
| Cluster cloud | `kafka-cloud` formaté `--standalone` avec le compte SCRAM `mm2` (`--add-scram`), sain | OK |
| Certificats | `generate-certs.sh` relancé sur un lab déjà préparé : certificat `kafka-cloud` signé par la CA existante (SAN `kafka-cloud`, `localhost`), CA et truststore inchangés | OK |
| ACL | Sans ACL : MM2 s'arrête sur `TopicAuthorizationException` (`mm2-offsets.onprem.internal`), le conteneur reste démarré. Avec 4 règles (préfixes `onprem.` et `mm2-`, topic `heartbeats`, groupe `*`) : MM2 démarre | OK |
| Réplication | 1 000 messages de `orders` recopiés dans `onprem.orders` en 17 s après le démarrage de MM2 | OK |
| Source intacte | Aucun topic `heartbeats` ni `mm2-*` sur le cluster onprem (après `cloud->onprem.emit.heartbeats.enabled = false`) | OK |
| Bascule d'un groupe | 300 messages lus sur la source, reprise sur le cloud : 797 lus pour 700 restants, soit 97 doublons, aucune perte. Test manuel : 20 messages lus sur une partition, offset traduit 1, 49 relus au lieu de 30 | OK |
| Configurations | `retention.ms` recopié sur `onprem.orders` ; passage de `payments` de 3 à 6 partitions recopié (test manuel) | OK |
| Coupure | MM2 arrêté, 2 000 messages produits : retard de 2 000 ; rattrapage en 11 s après redémarrage (JVM comprise). Test manuel : 5 000 messages, 12 s | OK |
| `validate.sh` | Heartbeats du cluster cloud en progression sur 5 s (422 puis 429) | OK |
| Mémoire | `kafka-cloud` 612 Mo, `mirrormaker` 407 Mo (`docker stats`, après les tests de réplication) | Mesuré |
| Notebooks du TP9 | Lab vierge, corrigés des TP1 à TP6 rejoués (`--rejouer`), puis `--executer demo-tp9 tp9` : aucune cellule en erreur, corrigé en 4 min 56 s (21:35 à 21:40). `--tester-enonce tp9` : erreurs seulement dans les vérifications | OK |
| Bascule, notebook | `orders` à 71 messages : 40 lus sur 3 partitions, 31 restants, 68 relus sur le cloud, 37 doublons (première exécution : 69 et 38) | OK |
| `offset.lag.max` | 6 partitions, groupe placé 100 messages avant la fin : défaut, groupe à jour 386 doublons (0 lors d'un autre essai), en retard 319 (331) ; à 10, à jour 0 (0), en retard 284 (117). Un groupe à jour ne relit presque rien avec 10 ; un groupe en retard relit des centaines de messages dans les deux cas (KIP-937) | Mesuré |
| Version en ligne de commande | Commandes de `enonce.md` rejouées (21:41 à 21:46) : sorties recopiées dans `corrige.md`. `orders` à 7 871 messages : 39 doublons ; rattrapage de 3 000 messages en moins de 13 s ; `kafka-consumer-groups.sh --reset-offsets --to-latest` puis `--shift-by -100` pour la variante | OK |
| Démonstration exploitation (J2) | Commandes de la slide rejouées : `audit` créé avec `--replica-assignment 1,2,3`, plan écrit à la main pour la partition 0 vers `[1,2,3]`, `--execute` puis `--verify` : réassignation terminée, ISR `1,2,3` | OK |

### Compléments issus d'*Apache Kafka in Action* (07/11/2026)

Lab vierge, mode `extended`, Kafka 4.3.2. Mesures sur macOS arm64, Docker Desktop.

| Sujet | Mesure | Où elle est citée |
|---|---|---|
| Ajout de partitions et clés | Topic `cles` à 3 partitions, clés `C001` à `C006`, puis passé à 5 : `C002` de la partition 0 à la 1, `C005` de la 2 à la 0, `C003` de la 2 à la 3, `C004` de la 2 à la 4 ; les anciens messages ne bougent pas | S8, TP2 (questions 4 et 6) |
| Membres statiques | Deux consommateurs `group.instance.id=inst-1` et `inst-2`, assignor `CooperativeStickyAssignor` : `inst-1` reçoit `orders:0,2,4`. `inst-2` arrêté depuis 5 s : groupe `Stable`, 2 membres, partitions conservées. Retour avec le même identifiant : mêmes partitions. 60 s après l'arrêt : `inst-1` détient les 6 partitions | S12, TP3 (variante 4) |
| Identifiant statique en double | Le plus ancien consommateur est rejeté : `FencedInstanceIdException` | TP3 (erreurs fréquentes) |
| Partitions à la limite du minimum ISR | Topic `atmin` (RF 3, `min.insync.replicas=2`) : `sum(kafka_cluster_partition_atminisr{topic="atmin"})` vaut 0 avec 3 brokers, 3 avec `kafka-3` arrêté depuis 50 s ; la somme `underminisr` du topic reste à 0 | S54, TP6 (variante 4) |
| Métriques exportées | Le lab exporte `kafka_cluster_partition_atminisr` (par partition) ; `AtMinIsrPartitionCount` et `OfflineLogDirectoryCount` ne sont pas exportés par les règles de l'agent JMX | S54, S61 |

Vérifié dans la documentation 4.3 : valeur par défaut de `partition.assignment.strategy` (`RangeAssignor,CooperativeStickyAssignor`), `group.instance.id`, `group.remote.assignor` et `group.consumer.assignors` (`uniform,range`), avertissements sur l'ajout de partitions (Operations, Modifying topics), tiered storage sans support des topics compactés, MBeans `AtMinIsrPartitionCount` et `OfflineLogDirectoryCount`. Non trouvé dans la documentation 4.3, donc présenté comme retour d'expérience : limite de latence de 30 ms entre brokers, minimum de 3 sites, remplacement d'un broker avec reprise de son `node.id` (non testé dans le lab).

Les consommateurs lancés en arrière-plan par un script ignorent `SIGINT` : pour les arrêter, utiliser `kill -TERM`. Au clavier, dans un terminal, `Ctrl+C` fonctionne.

### Incidents du TP8 (`incident.sh`)

| N° | Incident | Symptômes observés | Réparation |
|---|---|---|---|
| 1 | Arrêt brutal de kafka-2 | `Exited (137)`, URP 90 à 91 selon les topics présents, 1 partition sous min ISR (`refunds`), alerte `KafkaNodeDown` déclenchée 50 s après l'arrêt | OK : kafka-2 sain en 14 s, URP 0 (8 s lors d'un autre essai) |
| 2 | Quota `consumer_byte_rate=1024` | Premier essai : lag > 5000 en 70 s. Second essai (27/10) : 5,23 messages/s consommés contre 97,6 produits, throttle client 394 ms, throttle broker 7000 à 8300 ms, alerte `pending` au relevé fait 77 s après l'injection | OK : lag revenu vers 380 en 15 s |
| 3 | kafka-3 coupé du réseau | Conteneur toujours `healthy`, URP 85 à 90, kafka-3 absent des brokers, retard du quorum, `Error connecting to node kafka-1:9093`, `KafkaNodeDown` et `KafkaUnderMinIsrPartitions` déclenchées | OK : URP 0 en 7 s (premier essai), tout revenu en 25 s au plus (second essai) |
| 4 | Producteur mal configuré | Étape 1 : `OutOfMemoryError` côté client et `SSL handshake failed` côté broker. Étape 2 : `SCRAM-SHA-256 not enabled` | OK |
| 5 | `advertised.listeners` erroné (`kafka3`) | URP 31, sous min ISR 31, `UnknownHostException: kafka3`, `Expiring 4 record(s) for payments-2`, lignes `Shrinking ISR` sur kafka-3 | OK : `repair` en 13 s, URP 0 |
| 6 | Rétention infinie et rafale | Lab vierge (28/10) : pic de 294 Mo par broker, alerte `pending` vers 76 s, `firing` à 121 s. Lab chargé par les TP précédents (27/10) : 495 Mo par broker, `firing` à 91 s | OK : rétention 5 Mo par partition ; lab vierge : 22 Mo par broker et alerte levée en 31 s ; lab chargé : 248 Mo et alerte levée en 60 s |

### Compléments vérifiés pendant la rédaction des TP (27/10)

| Domaine | Vérification | Résultat |
|---|---|---|
| Traces | `broker-loggers --delete-config` ramène le logger au niveau du logger racine (`kafka.request.logger` : WARN, DEBUG, puis INFO après suppression) | OK, piège documenté au TP6 |
| Traces | Réglage fait par l'API perdu au redémarrage (`kafka.cluster.Partition` DEBUG, INFO après `docker compose restart kafka-1`) | OK |
| Traces | `org.apache.kafka.controller=DEBUG` : environ 20 lignes par seconde ; retour à INFO arrête le flux | OK |
| Groupes | Suppression d'un groupe actif refusée (`GroupNotEmptyException`) | OK |
| Supervision | Consommateur arrêté : `up{instance="loadgen-consumer"}=0`, métrique `records_lag_max` absente, fonctions `lag` et `alertes` de l'énoncé du TP6 | OK |
| Exploitation | Rolling restart au rythme manuel (pauses de 20 s) : 99 s, 90 000 envois `acks=all`, 0 erreur, 14 avertissements retriables, latence maximale 1122 ms | OK |
| Exploitation | Broker désenregistré puis redémarré : il se réenregistre (`unfenced`) ; observateur arrêté encore listé 3 min après, absent à 7 min | OK |
| Charge | Topic `loadgen` sur lab vierge (28/10) : `retention.bytes=10485760`, `segment.bytes=2097152`, `retention.ms=3600000`. Plafond calculé d'environ 72 Mo par broker (6 partitions de 10 Mo plus un segment actif de 2 Mo), atteint après 40 min de charge environ : calcul, non mesuré en régime établi | OK (configuration) |
| Outils | `reset.sh` retire une règle `tp6-custom.yml` laissée par le défi du TP6 et en garde une copie (fichier factice, 28/10) | OK |

### Démonstrations

| Démo | Vérification | Résultat |
|---|---|---|
| D2 | Client sur l'hôte via `localhost:9092` (produire, consommer) ; client hors réseau Docker en échec sur `localhost:9192` | OK |
| D3 | Connect : FileStream source et sink (20 lignes de bout en bout) | OK, après correction du volume (section 6) |
| D3 | Schema Registry : producteur et consommateur Avro, refus HTTP 409 (`READER_FIELD_MISSING_DEFAULT_VALUE`), version 2 acceptée avec `default` ; déroulé de `demos/d3-schema-registry-connect.md` rejoué en entier, y compris la tâche `FAILED` sous un connecteur `RUNNING` | OK |
| D4 | Perte de quorum (2 nœuds sur 3) : `describe --status` et création de topic bloqués 45 s, `--list` servi par le cache, `acks=1` possible sur le nœud restant ; création expirée appliquée au retour du quorum (2 essais) ; quorum revenu en 3 s, nœuds sains en 6 s | OK |
| D5 | Modification de `log4j2.yaml` (`kafka.request.logger` WARN vers DEBUG) prise en compte sur les 3 nœuds en 27 à 32 s (premier essai) puis 5 à 17 s (second essai, déroulé de `demos/d5-log4j2-a-chaud.md`) ; restauration en 28 s | OK |
| D6 | Commandes de la slide S28 rejouées telles quelles : 50 messages en `read_uncommitted`, 0 en `read_committed`, état `Ongoing`, puis `CompleteCommit`, 600 messages visibles, dernier offset 602 (2 marqueurs) | OK |
| D7 | `kafka-features` : `describe`, `version-mapping`, `upgrade --dry-run`, rétrogradation refusée (`--metadata 4.2` obsolète et `--release-version 4.2`) | OK |

### Supervision

- Les 32 requêtes PromQL du tableau de bord (27 panneaux et 7 rangées) renvoient des données en mode `extended`.
- `promtool check rules` : 9 règles valides.
- Affichage contrôlé le 28/10 dans Chrome (sans fenêtre, par Playwright), lab en mode `extended` : tableau de bord Grafana complet, 7 rangées remplies ; Kafka UI (Dashboard, Brokers, détail d'un broker, Topics, les 6 onglets d'un topic, Consumers, ACL) ; Prometheus (Query, Alerts, et les pages du menu Status). Relevés cités dans `formateur/visite-interfaces.md` : Fetch p99 d'environ 500 ms, Produce p99 d'environ 7 ms, lag `loadgen-consumers` d'environ 400 côté kafka-exporter et proche de 0 côté client, 20 892 séries dans Prometheus, cible `kafka-scale` DOWN tant que `kafka-4` n'est pas démarré.
- PDF client `livrables-client/Kafka-Administration-visite-des-interfaces.pdf` : 18 captures prises le 28/10 dans les mêmes conditions. Le PDF ne cite que des valeurs lisibles sur ses captures : 19 leaders et 57 répliques par broker, 171 répliques synchronisées sur 171, environ 33 messages par seconde et par broker, 256 octets par message, Produce p99 d'environ 7 ms, Fetch p99 d'environ 500 ms, lag d'environ 400.
- Non affichées : les rubriques Schema Registry et Kafka Connect de Kafka UI en mode `full`. Leurs libellés viennent du code de l'interface v1.5.0.

### Évaluation et outils

| Élément | Résultat |
|---|---|
| `final/prepare-final.sh` puis `check-final.sh` | 1/13 avant correction ; 5 alertes actives |
| Solution de référence appliquée, puis `check-final.sh` | 13/13, aucune alerte active (27/10, puis rejoué sur lab vierge le 28/10 : alertes levées 75 s après les corrections) |
| Erreurs probables des stagiaires au TP final (28/10) | Réassignation avec kafka-3 arrêté : `Unknown broker id 3`. `kafka-topics.sh --alter --replication-factor 3` : code retour 1, `Missing required argument "[partitions]"` |
| `offline-bundle.sh save` (`arm64` et `--platform linux/amd64`) | Lots de 1,6 Go ; architecture `amd64` vérifiée dans les configurations d'images |
| `offline-bundle.sh load` (lot `arm64`) | Images chargées en 15 s |
| `prepare.sh` | Agent JMX téléchargé, sha256 vérifié, certificats générés |

### Notebooks JupyterLab (28/10)

| Vérification | Résultat |
|---|---|
| `prepare.sh` : `pull --ignore-buildable`, puis construction de `kafka-lab/jupyter:1` | OK (5 s, image en cache) |
| Cycle `reset.sh --yes`, `start.sh extended`, `validate.sh` avec les 2 contrôles Jupyter (healthcheck, client Python connecté aux 3 brokers) | OK : 8 s, 22 s, 21 s |
| `docker compose` lancé depuis le conteneur `jupyter` (`ps`, `exec`, `logs`, `restart kafka-N`) | OK |
| Corrigé du TP1 et démo d'ouverture exécutés sur lab vierge (`build_notebooks.py --executer tp1 demo-tp1`) | 0 cellule en erreur, 35 s |
| Énoncé du TP1 exécuté avec ses `TODO` vides (`--tester-enonce`) | Seules les 9 cellules de vérification échouent (`ARevoir`) |
| Chaîne complète sur lab vierge en mode `extended`, TP après TP (`build_notebooks.py --tester-enonce --executer demo-tp1 tp1 ... demo-tp8 tp8`, 29/10, 00:30) | 8 démonstrations et 8 corrigés sans cellule en erreur, en 35 min 7 s ; énoncés : 75 vérifications en échec (`ARevoir`), aucune autre erreur. Valeurs citées « dans ce corrigé » relues et alignées sur cette exécution (TP2, TP4, TP6, TP7, TP8), puis textes reportés par `--textes` |
| Chaînes complètes précédentes du 28/10 | Quatre chaînes interrompues ou invalides, chacune à l'origine d'une correction de la section 6 : veille du Mac (capot fermé sur batterie), coordinateur de groupe en cache (TP3), alertes de fin du TP7 au début du TP8, broker 3 injoignable désigné comme contrôleur (TP8), sauvegarde de l'incident 5 non retirée par `reset.sh` |
| `describe_cluster().controller` comparé au leader du quorum (5 appels) | Valeur variable (exemples relevés : 1, 3, 2, 3, 2 avec un leader 2 ; 2, 3, 2, 1, 3 avec un leader 3) : les brokers renvoient un broker tiré au hasard |
| Redémarrage du contrôleur actif depuis le notebook | Nouveau leader observé 4,4 à 4,6 s après le lancement de `docker compose restart` (3 exécutions) |

Comportements du client Python (confluent-kafka 2.16.0) relevés pendant l'écriture des notebooks des TP4 à TP6, le 28/10 :

| Situation | Comportement observé |
|---|---|
| `acks=all`, ISR sous `min.insync.replicas` | Avec `retries=3` : `NOT_ENOUGH_REPLICAS` en moins de 1 s. Avec les réglages par défaut : `_MSG_TIMED_OUT` au bout de `message.timeout.ms` |
| Client en clair sur le listener SASL_SSL | `_ALL_BROKERS_DOWN` ; journal de niveau INFO `Disconnected: connection closed by peer ... in state APIVERSION_QUERY` (le client Java lève `OutOfMemoryError`) |
| Mauvais mot de passe SCRAM | `_AUTHENTICATION`, journal de niveau ERROR |
| `SCRAM-SHA-256` sur le listener SECURE | `_AUTHENTICATION` : `Unsupported SASL mechanism: broker's supported mechanisms: SCRAM-SHA-512` |
| Sans `ssl.ca.location` | `_SSL` : `certificate verify failed` |
| `ssl.truststore.location` (réglage Java) | `_INVALID_ARG` : `Java TrustStores are not supported` |
| `AclBinding` avec `ANY` | `ValueError: Cannot use enum operation, value ANY in this class` : `ANY` est réservé à `AclBindingFilter` |
| Consommateur sans ACL sur son groupe | `GROUP_AUTHORIZATION_FAILED`, avant toute erreur sur le topic |
| Producteur sans droit `Write` | Non idempotent (défaut de librdkafka) : `TOPIC_AUTHORIZATION_FAILED`. Idempotent : erreur fatale `CLUSTER_AUTHORIZATION_FAILED`, levée par `flush()` |
| Quota `producer_byte_rate=102400` sur `alice` | 0,32 à 0,33 Mo/s avec `batch.size=16384` et `sticky.partitioning.linger.ms=0` (le quota s'applique par broker, 3 leaders). Avec les réglages par défaut : 0,12 Mo/s, un seul broker reçoit à un instant donné |
| Groupe classique vide, sans offset validé | Supprimé par le broker : le TP5 valide ses offsets pour que `billing-secure` existe encore au TP6 |
| API absentes | Quorum, fonctionnalités, log dirs, réassignation, désenregistrement de broker, quotas, `broker-loggers`, `--entity-default` : passage par la CLI |
| `curl` | Absent des conteneurs `tools` et `jupyter` : `wget` dans `tools`, `requests` en Python |
| `AdminClient` qui a déjà servi, après un changement de coordinateur du groupe (rééquilibrage automatique des leaders, 5 min après le redémarrage d'un nœud) | Le client garde l'ancien coordinateur en cache : `describe_consumer_groups()` renvoie `NOT_COORDINATOR` pendant au moins 57 s, et `list_consumer_group_offsets()` arrête le processus (erreur de segmentation, code 139, reproduit 3 fois). Un client neuf obtient aussitôt le nouveau coordinateur. Constaté le 28/10 en fin de TP3, 5 min après le redémarrage de `kafka-2` au TP1, puis reproduit en déplaçant le leader de `__consumer_offsets-10`. Depuis, `admin()` crée un client neuf à chaque appel (100 clients en 0,4 s) : reproduction rejouée sans erreur |
| `describe_cluster()` et `describe_configs()` pendant l'incident 5 (Paiements : `kafka-3` annoncé sous le nom `kafka3`, absent du DNS) | Ces requêtes partent vers le broker que les métadonnées désignent comme contrôleur, tiré au hasard en KRaft. Quand c'est le broker 3, elles expirent : `_TIMED_OUT`, `Failed while waiting for controller`. Mesuré les 28 et 29/10 avec un client neuf à chaque essai : pour `describe_configs()`, 3 échecs sur 8, puis 1 sur 12 lors de deux autres séries ; pour `describe_cluster()`, 2 sur 8 ; `list_topics()` répond toujours. Sur 30 clients neufs, `list_topics().controller_id` vaut 3 14 fois ; un rafraîchissement des métadonnées toutes les secondes ne change rien. `bilan()` du TP8 lit donc les brokers dans les métadonnées et refait jusqu'à 8 essais de `sous_min_isr()` (délai de 5 s), en l'affichant |

### VS Code et serveur du lab (30/10)

Extension Jupyter 2025.9.1 sur macOS, lab en mode `extended`. Les requêtes de l'extension, lues dans son code et dans le journal du serveur, ont été rejouées en Python : l'interface de VS Code n'a pas été pilotée. Le cycle `reset.sh`, `start.sh`, `validate.sh`, `selftest.sh` n'a pas été rejoué après cette modification, pour ne pas effacer le lab en cours : seuls les 2 contrôles Jupyter de `validate.sh` l'ont été.

| Vérification | Résultat |
|---|---|
| Tentative depuis VS Code, serveur sans jeton (journal du serveur) | `GET /` 302, `GET /login?` 404, `GET /tree` 404 : pas de cookie XSRF pour l'extension |
| `POST /api/sessions` sans cookie XSRF, serveur sans jeton | 403, `'_xsrf' argument missing from POST` |
| Configuration du dépôt, `JUPYTER_TOKEN` vide | Comportement inchangé : `/lab` 200, API en lecture 200, racine limitée à `tps` et `demos-tp` |
| `JUPYTER_TOKEN=kafka-lab`, recréation du seul conteneur `jupyter` | Sain, nœuds Kafka non redémarrés, client Python connecté aux 3 brokers |
| Avec jeton, requête sans jeton | `/lab` 302 vers `/login`, API 403, `POST /api/sessions` 403 ; autre origine 404 ; `/api` 200, donc healthcheck inchangé |
| Avec jeton, `POST /api/sessions` en `Authorization: token`, sans cookie | 201. Noyau : dossier courant `/notebooks`, Python 3.13.15, `labkafka` importé, 3 brokers, `tools()`, `hote()` et `tableau()` fonctionnels. Fermeture sans noyau restant |
| Serveur jetable, `disable_check_xsrf=True` sans jeton | Session 201, autre origine 404. Non retenu : retire une protection sur un serveur qui a accès au socket Docker |

## 5. Non testé

| Élément | Risque | Recommandation |
|---|---|---|
| Windows 10/11 avec WSL2 | Performances des montages, fins de ligne des scripts | Tester un poste type avant la session ; cloner le dépôt dans WSL2, pas sous `/mnt/c` |
| Linux `x86_64` (Docker Engine sans Desktop) | Faible : images multi-architecture | Lancer `validate.sh` et `selftest.sh` sur un poste type |
| Chargement du lot `amd64` sur un PC `x86_64` | Faible | Tester `offline-bundle.sh load` sur un poste de la salle |
| Docker Desktop avec l'ancien magasin d'images (sans containerd) | `docker save --platform` indisponible | Constituer le lot sur une machine de même architecture que les postes |
| Proxy d'entreprise | `prepare.sh` peut échouer (GitHub, registres) | Utiliser le lot hors ligne |
| Postes avec 8 Go de RAM en mode `extended` | Possible pression mémoire | Utiliser le mode `core` et suivre la supervision sur le poste du formateur |
| Variante mTLS (keystore `client-alice`) | Configuration du listener à adapter (`ssl.client.auth`) | Variante avancée, non incluse dans le selftest |
| `offline-bundle.sh save` avec l'image JupyterLab, surtout pour une autre architecture (construction émulée) | Lot sans image JupyterLab, ou construction longue | Constituer le lot la veille et vérifier `docker image inspect kafka-lab/jupyter:1` après `load` |
| Conteneur `jupyter` sur Windows (WSL2) et Linux : montage au même chemin que l'hôte, socket Docker | `hote()` inutilisable si le chemin ou le socket diffère | Tester `hote("docker compose ps")` sur un poste type |
| Chemin de la formation contenant des espaces | Montage ou `docker compose` en échec dans le conteneur | Placer la formation dans un chemin sans espace |
| Parcours dans l'interface de VS Code : *Existing Jupyter Server...*, exécution de cellules | Écart possible avec les requêtes rejouées le 30/10 | Essayer sur le poste du formateur avant la session |

## 6. Anomalies trouvées et corrigées pendant les tests

| Anomalie | Cause | Correction |
|---|---|---|
| `validate.sh` : MirrorMaker 2 déclaré arrêté alors qu'il réplique (heartbeats de `onprem.heartbeats` figés, 04/11) | `onprem.heartbeats` recopie les heartbeats émis sur la source par le sens inverse, désactivé | Contrôle sur le topic `heartbeats` du cluster cloud, alimenté par le flux actif |
| MM2 crée `heartbeats` et `mm2-*.cloud.internal` sur le cluster source (04/11) | En mode dédié, un sens désactivé émet quand même des heartbeats | `cloud->onprem.emit.heartbeats.enabled = false` dans `mm2.properties` |
| Variante du TP9 : `offset.lag.max = 10` donne plus de doublons que la bascule de 7.5 (721 contre 37, 04/11) | Comparaison faussée : deux scénarios différents, et attente arrêtée dès qu'une seule partition était traduite (les autres relues depuis le début) | Attente de toutes les partitions ; variante refaite en quatre bascules comparables |
| Énoncé du TP9 : cellules TODO 7.4 et 7.7 et bilan en erreur quand le cluster cloud n'est pas démarré | Affichage qui interrogeait le cluster cloud | Affichages sans appel au cluster cloud ; bilan protégé |
| Textes du TP9 et slide S75 : `onprem.heartbeats` annoncé sur le cluster cloud | Relevé fait avant `cloud->onprem.emit.heartbeats.enabled = false` | Liste corrigée sur la sortie réelle |
| Schema Registry jamais déclaré sain | L'image `cp-schema-registry:8.3.2` ne contient ni `curl`, ni `wget`, ni `nc` | Healthcheck en Python (`urllib`) |
| Connecteur FileStream sink en échec : `Couldn't find or create file` | Volume monté sur un chemin absent de l'image, donc créé avec root comme propriétaire | Volume monté sur `/var/lib/kafka`, qui appartient à `appuser` dans l'image |
| Alerte disque jamais déclenchée par l'incident 6 | Seuil de 400 Mo supérieur aux 380 Mo atteints | Seuil de lab abaissé à 250 Mo |
| Logger `kafka.controller` refusé (`Invalid broker logger`) | Nom de logger de Kafka 3.x | `org.apache.kafka.controller` |
| Recul `--shift-by` sans effet sur une partition | Messages sans clé envoyés en un seul lot dans une seule partition (partitionneur « sticky ») | Recul appliqué à toutes les partitions ; comportement expliqué au TP3 |
| Nouveau groupe sans message lu | Un nouveau groupe démarre à `latest` par défaut | `--from-beginning` dans les commandes du TP3 |
| Volume de `loadgen` en dents de scie entre 100 et 200 Mo par broker | Segments de 16 Mo : la rétention ne supprime que des segments entiers | `retention.bytes` de 10 Mo par partition et segments de 2 Mo |
| Alerte disque toujours active après la réparation de l'incident 6 | Réparation à 20 Mo par partition, trop haute sur un lab chargé | Réparation à 5 Mo par partition |
| `KafkaException ... _DESTROY` sur `AdminClient(...).describe_cluster().result()` (notebook et `validate.sh`, 28/10) | Client Python détruit avant la réponse, faute de référence | Client gardé dans une variable ; `admin()` de `labkafka.py` garde en mémoire ses 32 derniers clients |
| Sortie des commandes affichée deux fois dans les notebooks | Jupyter affiche aussi la valeur renvoyée par `tools()` ou `hote()` en fin de cellule | `Sortie` n'est réaffichée que si elle ne l'a pas été |
| Messages d'information de librdkafka (niveau 6, `FAIL ... Disconnected`) dans la sortie du redémarrage du contrôleur | Niveau de log par défaut de librdkafka (informations) | `log_level` 4 pour les clients de `admin()` : avertissements et erreurs seulement |
| Lignes `%3|...|FAIL|... Failed to resolve 'kafka-3:19092'` répétées sous les cellules du TP4 pendant l'arrêt de `kafka-3` (corrigé exécuté le 28/10, 8 lignes) | Avec un client neuf à chaque appel de `admin()`, chaque client tente de joindre le broker arrêté et l'écrit dans son journal | `log_level` 2 pour les clients de `admin()` ; les erreurs des requêtes arrivent par les futurs. Les lignes des producteurs et consommateurs des TP restent visibles : au TP8, elles font partie des symptômes |
| Rétention du TP3 : premier offset d'`audit-short` passé directement de 0 à 101, vérification 7.8 en échec (corrigé exécuté le 28/10) | Le segment actif est supprimé lui aussi quand son message le plus récent a expiré : le broker ouvre un segment vide, puis supprime l'ancien. Avec 12 s entre les deux envois, la vérification du leader (toutes les 30 s) est passée après l'expiration des deux segments (traces : `Incremented log start offset to 101`, `Rolled new log segment at offset 101`) | 35 s entre le 100e et le 101e message, plus que la période de vérification : mesuré 0 -> 100 à 84-96 s après le 100e message, puis 101 environ 30 s plus tard. Vérification 7.8 acceptant 100 ou 101 ; `selftest.sh` attend lui aussi 35 s après les 100 messages. 101 messages dans un seul segment : 0 -> 101 directement, à 89 s |
| Cellule de bilan du TP3 en échec : `NOT_COORDINATOR` sur `describe_consumer_groups()`, y compris après 30 s de nouveaux essais (corrigé exécuté deux fois le 28/10) | Le contrôleur rééquilibre les leaders 5 min après le redémarrage de `kafka-2` au TP1 ; le coordinateur du groupe change de broker, et l'`AdminClient` unique de `admin()` le garde en cache (voir le comportement du client en section 4) | `admin()` crée un client neuf à chaque appel ; nouvel essai retiré de la cellule de bilan |
| Corrigé du TP8 en échec à la vérification 7.1 : alertes `KafkaFencedBrokers` et `KafkaUnderMinIsrPartitions` en attente (chaîne complète du 28/10) | Le TP8 démarre quelques secondes après la fin du TP7, dont l'arrêt de `kafka-4` laisse ces alertes `pending` une trentaine de secondes ; en formation, la pause les sépare | Le corrigé attend qu'aucune alerte ne soit active avant la référence ; l'énoncé dit d'attendre 30 s et de relancer `bilan()` |
| Corrigé du TP8 en échec sur `bilan()` pendant l'incident 5 : `KafkaException _TIMED_OUT` dans `sous_min_isr()` (chaîne complète du 28/10) | `describe_configs()` visait le broker 3, injoignable sous son nom annoncé (voir le comportement du client en section 4). Avec l'ancien client unique, le broker visé restait celui du premier tirage, souvent un autre | Brokers lus dans les métadonnées ; jusqu'à 8 essais de `sous_min_isr()`, délai de 5 s (3 essais à 10 s ont tous échoué lors d'une chaîne, le 29/10) ; symptôme expliqué dans le corrigé et dans les erreurs fréquentes |
| Incident 5 refusé après `reset.sh` : `[FAIL] Incident 5 deja injecte (sauvegarde presente)` ; les cellules du ticket Paiements s'exécutent alors sans incident (chaîne complète du 28/10) | Un corrigé interrompu après l'injection avait laissé `config/kafka/kafka-3/server.properties.before-incident5`. `reset.sh` restaurait `server.properties`, mais pas cette sauvegarde | `reset.sh` déplace aussi les sauvegardes `*.before-incident*` dans `config/kafka/backup-*` ; vérifié le 28/10 : injection sans réparation, `reset.sh`, puis injection et réparation de nouveau acceptées |


## Livraison par images préconstruites — 04/11/2026

Essais sur le moteur Docker Desktop existant (`linux/arm64`, 16 Go alloués).
Les versions de Kafka, des services et des bibliothèques Python restent inchangées.
Les services des autres projets, dont GitLab, ont été conservés. Pour éviter les
ports occupés par le mini-projet, `KAFKA_UI_PORT=18080` et `CONNECT_PORT=18083`
ont été utilisés uniquement pendant les démarrages de vérification.

| Vérification réellement exécutée | Résultat |
|---|---|
| `build-images.sh --platform linux/amd64,linux/arm64` | JupyterLab puis image Kafka commune construites et chargées localement ; présence des quatre variantes vérifiée avec `docker image inspect --platform` |
| Images `amd64` sous émulation, sans réseau | Imports JupyterLab 4.6.4, confluent-kafka 2.16.0, requests 2.34.2 ; CLI Kafka : `4.3.2`. Le test de CLI avec le nom d'hôte `localhost` résolu localement réussit sans l'avertissement DNS de la première commande isolée |
| `prepare.sh --no-pull` | Agent JMX présent dans l'image, SHA-256 correct, imports Python réussis ; aucune construction |
| `reset.sh --yes`, `start.sh full`, `validate.sh` | Démarrage complet avec les images préconstruites ; « Environnement valide. » ; JupyterLab, supervision, charge, Schema Registry et Connect vérifiés |
| `selftest.sh` complet | **97 vérifications réussies, aucun échec, 470 s** ; inclut SASL_SSL, ACL, métriques JMX, `kafka-4`, réassignation, rolling restart, déplacement entre log dirs et décommissionnement |
| `python3 -m unittest discover -s lab/tests -v` | 4 tests réussis : exclusion des fichiers réservés et secrets, empreintes, reprise des configurations d'origine, refus des liens symboliques, des versions implicites et des TP incomplets |
| Génération de l'archive client `2026.11.04-1` | 66 fichiers, dont `livraison.json` ; empreintes des 65 fichiers du manifeste vérifiées ; aucun certificat, corrigé, source de notebook ni `.env` |
| `offline-bundle.sh save`, pour les deux architectures | Lots générés avec les sept images de tous les profils, dont `scale`, liste des références et empreinte SHA-256 |
| Installation depuis l'archive extraite dans un dossier neuf | Chargement du lot arm64, `prepare.sh --no-pull`, génération des certificats absents, `start.sh full`, `validate.sh` : « Environnement valide. » ; nouvelle CA distincte de celle du poste source |

Dans le JupyterLab de cette installation, vérification supplémentaire réussie :
seuls `tps/` et `demos-tp/` sont visibles ; `hote()` résout le Compose de l'archive,
`tools()` exécute la CLI et le client Python découvre les trois brokers.

Tailles mesurées le 04/11/2026 : archive de supports 3 699 073 octets ; lot amd64
1 806 246 063 octets ; lot arm64 1 753 381 047 octets. Les fichiers sont dans
`dist/` (artefacts locaux exclus de Git). Les deux images du lab portent les
références locales `kafka-lab/jupyter:1` et `kafka-lab/kafka:4.3.2-1`.

Remise à zéro finale exécutée pour l'installation extraite puis les sources :
aucun conteneur du projet `kafka-lab` restant. Les conteneurs du mini-projet et
GitLab sont toujours en marche.

La première livraison ci-dessus utilisait des références locales ; la publication
sur Docker Hub est décrite dans la section suivante. La construction et les essais sous émulation ne constituent pas une
validation sur un vrai poste Windows/WSL2 ou Linux amd64 vierge.


## Publication Docker Hub — 04/11/2026

Compte choisi : `m0uaad`, déjà configuré dans Docker Desktop. Le compte GitHub
`formateurdata` est connecté, mais son accès CLI n'a pas le droit `write:packages`.
Aucun identifiant n'a été ajouté aux supports ni aux images.

Les références par défaut de Compose, de `env.template` et des scripts sont
`m0uaad/kafka-lab-jupyter:1` et `m0uaad/kafka-lab-kafka:4.3.2-1`. Les logiciels
et les configurations Kafka restent identiques à la livraison locale précédente.
Le générateur d'archives accepte désormais ces références sans dépendre des
anciennes valeurs par défaut locales ; ses quatre tests restent réussis.

Cycle complet rejoué avec les nouveaux noms, à partir des images locales :
`reset.sh --yes`, `start.sh full`, `validate.sh`, `selftest.sh`, `reset.sh --yes`.
Résultat : **97 vérifications réussies, aucun échec, 441 s**, puis aucun conteneur
ou volume de données du lab laissé en fonctionnement. Les ports alternatifs
18080 et 18083 ont été conservés pour les autres projets du poste.

L'envoi direct par Buildx a échoué sur des délais réseau et une résolution DNS
indisponible dans Docker Desktop. Les mêmes images ont été exportées en archives
OCI multiplateformes, puis envoyées depuis macOS avec `regctl` 0.11.7 temporaire
(empreinte SHA-256 du binaire vérifiée depuis la release officielle). La
configuration temporaire utilise le gestionnaire d'identifiants Docker existant ;
aucun mot de passe n'a été copié dans les supports.

Livraison des supports : `dist/Kafka-Administration-2026.11.04-2.tar.gz` et son
empreinte SHA-256. Les lots correspondants sont dans
`dist/hors-ligne-2026.11.04-2/`. Ne pas mélanger leurs listes de références avec les
lots de la première livraison locale.


Publication terminée, accès anonyme vérifié par l'API du registre : les deux
index et leurs manifestes `linux/amd64` et `linux/arm64` sont lisibles sans
identifiants. Les manifestes et empreintes sont enregistrés dans
`dist/publication-dockerhub.json`.

| Image publiée | Digest de l'index OCI |
|---|---|
| `m0uaad/kafka-lab-jupyter:1` | `sha256:d82d8aa9435089b2801617c7096c0d6d41d844dd9185783fcc0ac613c91147bb` |
| `m0uaad/kafka-lab-kafka:4.3.2-1` | `sha256:9940cfd9cfd733bd963355e74392adf1be6797252d0e65e1f46fd78612deff83` |

Les dernières couches de base Kafka ont été réutilisées depuis le dépôt Apache.
Les manifestes OCI Kafka ont été finalisés par l'API du registre à partir des
exports locaux inchangés ; leurs digests correspondent aux fichiers exportés.
Les anciens transferts ont été arrêtés après vérification publique.

Les lots ont été régénérés à partir des exports OCI portant les noms Docker Hub
et des images officielles déjà sauvegardées pour chaque architecture, avec
`docker load` puis `docker save --platform`. Le lot arm64 de cette livraison a
été chargé depuis les supports extraits dans un nouveau dossier :
`prepare.sh --no-pull`, création des certificats, `start.sh full`, `validate.sh`,
contrôle de `hote()` et du client Python depuis JupyterLab, puis `reset.sh --yes`.
Résultat : « Environnement valide. » Aucun conteneur `kafka-lab` restant ; GitLab
est toujours en fonctionnement.

Le chargement sur un vrai poste amd64 et Windows/WSL2 reste à valider. L'accès
anonyme testé porte sur les manifestes distants ; l'installation complète rejouée
sur ce Mac a utilisé le lot hors ligne.
