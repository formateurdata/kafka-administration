# TP1 : déployer et valider le cluster KRaft

| Repère | Valeur |
|---|---|
| Moment | Jour 1, 11:05-11:45, après la démo d'ouverture du TP (11:00-11:05) |
| Compétences | C2 KRaft et quorum ; C3 Déployer, démarrer et valider un cluster |
| Slides de rappel | S18 (installation), S19 (`server.properties`), S24 (architecture du lab) |
| Solution formateur | `corrige.md` (point 13) |
| Version notebook | `enonce.ipynb` dans JupyterLab (http://localhost:8888), mêmes étapes en Python ; démo d'ouverture : `demos-tp/demo-tp1.ipynb` |

Conventions des commandes :

- `# [hôte]` : sur votre machine, dans le dossier `lab/` ;
- `# [tools]` : dans le shell d'administration, ouvert par `docker compose exec tools bash` ;
- `$BS` vaut `kafka-1:19092,kafka-2:19092,kafka-3:19092` dans `tools`.

Les sections 7, 8 et 9 utilisent la même numérotation : l'étape 7.4 se fait avec les commandes 8.4 et donne le résultat 9.4.

---

## 1. Objectifs

À la fin du TP, vous savez :

- démarrer le lab et vérifier que les 3 nœuds sont sains ;
- lire dans `server.properties` ce qui fait d'un nœud un broker et un contrôleur KRaft ;
- vérifier qu'un nœud a été formaté et à quel cluster il appartient ;
- identifier le contrôleur actif et l'état de réplication du quorum ;
- vérifier que les brokers sont enregistrés et lire les fonctionnalités activées (`metadata.version`) ;
- retrouver un événement dans les traces d'un nœud ;
- lire le journal de métadonnées `__cluster_metadata` ;
- constater qu'un redémarrage du contrôleur actif provoque une nouvelle élection sans interruption.

## 2. Durée

40 minutes, après 5 minutes de démonstration d'ouverture.

| Étape | Durée |
|---|---|
| 7.1 Démarrer et valider le lab | 4 min |
| 7.2 Lire la configuration KRaft d'un nœud | 5 min |
| 7.3 Vérifier le formatage du stockage | 4 min |
| 7.4 Interroger le quorum | 5 min |
| 7.5 Brokers enregistrés et fonctionnalités | 5 min |
| 7.6 Traces : retrouver l'élection du contrôleur (défi) | 5 min |
| 7.7 Journal de métadonnées | 4 min |
| 7.8 Kafka UI | 2 min |
| 7.9 Redémarrer le contrôleur actif | 6 min |

## 3. Prérequis

- Docker Desktop démarré, avec 6 Go de mémoire allouée pour le mode `extended` (4 Go pour le mode `core`).
- Le lab préparé la veille par le formateur : `scripts/prepare.sh` a été exécuté.
- Un terminal ouvert dans le dossier `lab/`.
- Notions : slides S4 à S24 (topics, partitions, brokers, KRaft, listeners).

## 4. Architecture utilisée

```
                         Réseau Docker kafka-lab
  +----------------------------------------------------------------------+
  |                                                                      |
  |   kafka-1 (az-a)          kafka-2 (az-b)          kafka-3 (az-c)     |
  |   broker + contrôleur     broker + contrôleur     broker + contrôleur |
  |   node.id=1               node.id=2               node.id=3          |
  |        \                       |                       /             |
  |         +------ quorum KRaft : 3 votants (port 9093) -+              |
  |                                                                      |
  |   tools : poste d'administration (kafka-*.sh, $BS)                   |
  |   kafka-ui : http://localhost:8080                                   |
  |   [extended] prometheus, grafana, kafka-exporter, loadgen            |
  +----------------------------------------------------------------------+
```

| Listener | Port | Protocole | Usage dans ce TP |
|---|---|---|---|
| CONTROLLER | 9093 | PLAINTEXT | Quorum KRaft, entre contrôleurs |
| INTERNAL | 19092 | PLAINTEXT | Outils du conteneur `tools` (`$BS`), réplication |
| EXTERNAL | 9092, 9192, 9292 | PLAINTEXT | Clients lancés sur l'hôte (non utilisé ici) |
| SECURE | 19094 | SASL_SSL | TP5 |

## 5. Contexte métier

NovaShop, site de commerce en ligne, migre ses flux de commandes, de paiements et de navigation vers Kafka 4.3. L'équipe plateforme, dont vous faites partie, reçoit le nouveau cluster de recette : 3 nœuds en mode KRaft, sans ZooKeeper.

Avant d'ouvrir le cluster aux équipes de développement, vous devez produire un procès-verbal de recette technique. Il doit prouver que le cluster est sain, que le quorum fonctionne, que les traces sont exploitables et qu'un redémarrage de nœud ne l'interrompt pas.

## 6. Énoncé

Démarrez le cluster, puis collectez les preuves suivantes pour votre procès-verbal :

1. l'identifiant du cluster et le fait que chaque nœud a été formaté avec cet identifiant ;
2. l'identité du contrôleur actif et l'état de réplication du quorum ;
3. la liste des brokers enregistrés et la version de métadonnées en vigueur ;
4. la trace qui prouve l'élection du contrôleur actif ;
5. le comportement du cluster quand le contrôleur actif redémarre.

Notez chaque preuve dans la grille de la section 10.

## 7. Étapes

**7.1 Démarrer et valider le lab.** Lancez le lab en mode `extended` (ou `core` si votre poste a moins de 16 Go de RAM), puis le script de validation. Listez les conteneurs.

**7.2 Lire la configuration KRaft d'un nœud.** Repérez dans `config/kafka/kafka-1/server.properties` les paramètres qui définissent le rôle du nœud, son identifiant, le quorum, les listeners et le répertoire de données.

**7.3 Vérifier le formatage du stockage.** Affichez les informations de stockage de `kafka-1` et le fichier `meta.properties`. Retrouvez dans les traces de démarrage la ligne de formatage.

**7.4 Interroger le quorum.** Dans `tools`, affichez l'état du quorum et sa réplication. Notez le `LeaderId`, le `LeaderEpoch` et le retard (`Lag`) des followers.

**7.5 Brokers enregistrés et fonctionnalités.** Vérifiez que les 3 brokers sont enregistrés et non isolés (`isFenced: false`). Affichez l'identifiant du cluster, les points de connexion et les fonctionnalités finalisées.

**7.6 Défi : retrouver l'élection du contrôleur dans les traces.** Listez les fichiers de traces d'un nœud. Puis trouvez sur quel nœud figure la ligne `Becoming the active controller` et vérifiez qu'il s'agit bien du `LeaderId` noté à l'étape 7.4. Indice : les fichiers de traces tournent toutes les heures, cherchez dans `controller.log*`.

**7.7 Explorer le journal de métadonnées.** Listez le contenu de la partition `__cluster_metadata-0` sur `kafka-1`, puis décodez son premier segment pour compter les types d'enregistrements et retrouver la création des topics.

**7.8 Kafka UI.** Ouvrez http://localhost:8080 : vérifiez le nombre de brokers, le contrôleur et la liste des topics.

**7.9 Redémarrer le contrôleur actif.** Redémarrez le nœud dont l'identifiant est le `LeaderId`. Observez immédiatement le nouveau leader du quorum, puis le retour du nœud.

## 8. Commandes directement exécutables

### 8.1 Démarrer et valider le lab

```bash
# [hôte]
scripts/start.sh extended        # ou : scripts/start.sh core
scripts/validate.sh
docker compose ps --format 'table {{.Name}}\t{{.Status}}'
```

### 8.2 Lire la configuration KRaft d'un nœud

```bash
# [hôte]
grep -E '^(process.roles|node.id|controller.quorum.bootstrap.servers|controller.listener.names|listeners|advertised.listeners|log.dirs)=' \
  config/kafka/kafka-1/server.properties
```

### 8.3 Vérifier le formatage du stockage

```bash
# [hôte]
docker compose exec kafka-1 /opt/kafka/bin/kafka-storage.sh info -c /opt/lab/node/server.properties
docker compose exec kafka-1 cat /var/lib/kafka/data/meta.properties
docker compose logs kafka-1 | grep -E 'start-node|Formatting|already formatted|Kafka Server started'
```

### 8.4 Interroger le quorum

```bash
# [hôte]
docker compose exec tools bash
# [tools]
echo $BS
kafka-metadata-quorum.sh --bootstrap-server $BS describe --status
kafka-metadata-quorum.sh --bootstrap-server $BS describe --replication
```

### 8.5 Brokers enregistrés et fonctionnalités

```bash
# [tools]
kafka-broker-api-versions.sh --bootstrap-server $BS | grep '^kafka'
kafka-cluster.sh cluster-id --bootstrap-server $BS
kafka-cluster.sh list-endpoints --bootstrap-server $BS
kafka-features.sh --bootstrap-server $BS describe
kafka-topics.sh --bootstrap-server $BS --list
```

### 8.6 Traces (défi)

```bash
# [hôte]
docker compose exec kafka-1 ls /opt/kafka/logs
docker compose exec kafka-1 grep 'Kafka Server started' /opt/kafka/logs/server.log
# Défi : écrivez la commande qui indique, pour chaque nœud, s'il contient la ligne
# "Becoming the active controller" dans ses fichiers controller.log*.
```

### 8.7 Journal de métadonnées

```bash
# [hôte]
docker compose exec kafka-1 ls /var/lib/kafka/data/__cluster_metadata-0
# Nombre d'enregistrements par type
docker compose exec kafka-1 sh -c '/opt/kafka/bin/kafka-dump-log.sh --cluster-metadata-decoder \
  --files /var/lib/kafka/data/__cluster_metadata-0/00000000000000000000.log 2>/dev/null \
  | grep -o "\"type\":\"[A-Z_]*\"" | sort | uniq -c | sort -rn'
# Création des topics
docker compose exec kafka-1 sh -c '/opt/kafka/bin/kafka-dump-log.sh --cluster-metadata-decoder \
  --files /var/lib/kafka/data/__cluster_metadata-0/00000000000000000000.log 2>/dev/null \
  | grep -o "payload: {\"type\":\"TOPIC_RECORD\".*" | head -3'
```

### 8.8 Kafka UI

Ouvrez http://localhost:8080, puis les menus « Brokers » et « Topics ». Équivalent en ligne de commande :

```bash
# [hôte]
curl -s localhost:8080/api/clusters
```

### 8.9 Redémarrer le contrôleur actif

Remplacez `N` par le `LeaderId` lu à l'étape 7.4.

```bash
# [hôte]
docker compose restart kafka-N
docker compose exec tools kafka-metadata-quorum.sh --bootstrap-server kafka-1:19092,kafka-2:19092,kafka-3:19092 describe --status
docker compose ps kafka-N
# Attendre que kafka-N soit "healthy" (environ 15 s), puis :
docker compose logs kafka-N --since 1m | grep -E 'start-node|already formatted|Kafka Server started'
docker compose exec tools kafka-metadata-quorum.sh --bootstrap-server kafka-1:19092,kafka-2:19092,kafka-3:19092 describe --replication
```

## 9. Résultats attendus

Les identifiants de répertoire, les offsets et les horodatages varient. Le `LeaderId` peut être 1, 2 ou 3.

### 9.1 Démarrer et valider le lab

`validate.sh` se termine par :

```
[ OK ] Environnement valide.
```

`docker compose ps` affiche `kafka-1`, `kafka-2` et `kafka-3` en `Up ... (healthy)`, ainsi que `jupyter`, `kafka-tools` et `kafka-ui`. En mode `extended` s'ajoutent `prometheus`, `grafana`, `kafka-exporter`, `loadgen-producer` et `loadgen-consumer`.

### 9.2 Configuration KRaft

```
process.roles=broker,controller
node.id=1
controller.quorum.bootstrap.servers=kafka-1:9093,kafka-2:9093,kafka-3:9093
controller.listener.names=CONTROLLER
listeners=CONTROLLER://:9093,INTERNAL://:19092,EXTERNAL://:9092,SECURE://:19094
advertised.listeners=CONTROLLER://kafka-1:9093,INTERNAL://kafka-1:19092,EXTERNAL://localhost:9092,SECURE://kafka-1:19094
log.dirs=/var/lib/kafka/data
```

### 9.3 Formatage

```
Found metadata: {cluster.id=W5FmqobtRp-BMcXlZhVKBw, directory.id=4EubdzSZTbykbeqlrbgz1Q, node.id=1, version=1}
```

Au premier démarrage, les traces contiennent `Formatting dynamic metadata voter directory /var/lib/kafka/data with metadata.version 4.3-IV0.` Aux démarrages suivants : `All of the log directories are already formatted.`

### 9.4 Quorum

```
ClusterId:              W5FmqobtRp-BMcXlZhVKBw
LeaderId:               2
LeaderEpoch:            1
...
CurrentVoters:          [{"id": 1, ...}, {"id": 2, ...}, {"id": 3, ...}]
CurrentObservers:       []
```

`describe --replication` affiche 1 ligne `Leader` et 2 lignes `Follower`, avec un `Lag` de 0 ou proche de 0.

### 9.5 Brokers et fonctionnalités

- 3 lignes `kafka-N:19092 (id: N rack: az-x isFenced: false)`.
- `Cluster ID: W5FmqobtRp-BMcXlZhVKBw`.
- `list-endpoints` : 3 brokers `unfenced`.
- `metadata.version` finalisée à `4.3-IV0`, `kraft.version` à 1.
- Topics : `__consumer_offsets`, plus `loadgen` en mode `extended`.

### 9.6 Traces

7 fichiers de traces, dont `server.log`, `controller.log` et `state-change.log`. La ligne `Becoming the active controller at epoch 1` n'apparaît que sur le nœud élu leader au démarrage.

### 9.7 Journal de métadonnées

Un segment `00000000000000000000.log`, un fichier `quorum-state` et un instantané `.checkpoint`. Les types d'enregistrements les plus nombreux sont `NO_OP_RECORD` et `PARTITION_RECORD`. On trouve un `TOPIC_RECORD` par topic.

### 9.8 Kafka UI

Cluster `novashop-lab` à l'état `ONLINE` avec 3 brokers.

### 9.9 Redémarrage du contrôleur actif

- Le redémarrage dure quelques secondes.
- Immédiatement après, `LeaderId` désigne un autre nœud et `LeaderEpoch` a augmenté de 1.
- Le nœud redémarré revient `healthy` en 15 à 20 s. Ses traces affichent `All of the log directories are already formatted.` puis `Kafka Server started`.
- `describe --replication` le montre de nouveau `Follower` avec un `Lag` de 0.

## 10. Vérification

Remplissez la grille du procès-verbal. Chaque ligne doit être prouvée par une sortie de commande.

| Preuve | Valeur relevée | Commande |
|---|---|---|
| Identifiant du cluster | | |
| Nœuds formatés avec cet identifiant (3/3) | | |
| Contrôleur actif et époque | | |
| Votants du quorum | | |
| Brokers enregistrés et non isolés | | |
| `metadata.version` | | |
| Nœud portant la trace d'élection | | |
| Nouveau contrôleur après redémarrage | | |

Contrôle final rapide :

```bash
# [hôte]
scripts/validate.sh | tail -1
```

## 11. Questions de réflexion

1. Pourquoi le quorum compte-t-il 3 votants plutôt que 2 ou 4 ? Combien de pannes de contrôleur chaque taille tolère-t-elle ?
2. Pourquoi `advertised.listeners` diffère-t-il de `listeners` ? Que se passerait-il si un nœud annonçait un nom d'hôte inexistant ?
3. Où la liste des topics est-elle stockée en mode KRaft ? Qui l'écrit ?
4. Au redémarrage du contrôleur actif, les producteurs et consommateurs sont-ils interrompus ? Pourquoi ?
5. Dans ce lab, chaque nœud est à la fois broker et contrôleur. Quels risques ce choix présente-t-il en production ?
6. Pourquoi le formatage du stockage est-il une étape explicite en KRaft ?

## 12. Erreurs fréquentes

| Symptôme | Cause | Correction |
|---|---|---|
| `command not found: kafka-topics.sh` | Commande lancée sur l'hôte | Ouvrir `docker compose exec tools bash` |
| `no configuration file provided: not found` | `docker compose` lancé hors du dossier `lab/` | `cd lab` |
| `--bootstrap-server must be specified` | Option oubliée | Ajouter `--bootstrap-server $BS` |
| `Connection to node -1 (localhost/127.0.0.1:9092) could not be established` | `localhost` désigne le conteneur `tools` lui-même | Utiliser `$BS` dans `tools` |
| `grep` ne trouve pas `Becoming the active controller` | Recherche dans `controller.log` seul, alors que la ligne est dans un fichier déjà tourné, ou sur un nœud non élu | Chercher dans `controller.log*` sur les 3 nœuds |
| Un nœud reste `health: starting` | Mémoire Docker insuffisante | Passer en mode `core` ; `docker compose logs kafka-N --tail 50` |

## 13. Solution formateur

Voir `corrige.md`.

## 14. Variante avancée

Pour les participants en avance :

1. Arrêtez proprement tout le lab avec `scripts/stop.sh`, puis redémarrez-le. Vérifiez que le topic `loadgen` et ses données sont toujours présents (`kafka-get-offsets.sh`). Expliquez pourquoi le formatage ne s'est pas reproduit.
2. Mesurez le temps nécessaire pour que le quorum élise un nouveau leader quand le contrôleur actif est arrêté brutalement (`docker compose kill kafka-N`) au lieu d'être redémarré proprement. Redémarrez ensuite le nœud avec `docker compose start kafka-N`.
3. Cherchez dans `kafka-features.sh --help` la commande qui afficherait les niveaux de fonctionnalités associés à une version de Kafka (`version-mapping`), et lancez-la pour la version 4.3.
