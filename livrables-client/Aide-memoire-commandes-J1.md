# Aide-mémoire : les commandes du jour 1

Ce document reprend les commandes du jour 1 (TP1 à TP4 et démonstrations), classées par thème, avec leur rôle. Les blocs « En plus » ajoutent des commandes utiles absentes des TP. Elles ont été vérifiées sur le lab (Kafka 4.3.2) le 16/11/2026.

## 0. Où taper les commandes

- `# [hôte]` : sur votre machine, dans un terminal ouvert dans le dossier `lab/`. C'est là que se lancent `docker compose` et les scripts `scripts/*.sh`.
- `# [tools]` : dans le shell d'administration, ouvert par `docker compose exec tools bash`. C'est là que se lancent tous les outils `kafka-*.sh`.
- `# [serveur]` : sur une machine où Kafka est installé à partir de l'archive officielle, hors du lab.
- `$BS` (bootstrap servers) vaut `kafka-1:19092,kafka-2:19092,kafka-3:19092` dans `tools`. Un outil contacte d'abord l'un de ces brokers, qui lui renvoie la carte complète du cluster.
- Chaque outil `kafka-*.sh` affiche ses options avec `--help`.

---

## 1. Piloter le lab

### Démarrer, vérifier, arrêter

```bash
# [hôte]
scripts/start.sh core        # 3 noeuds Kafka, tools, Kafka UI, JupyterLab
scripts/start.sh extended    # core + Prometheus, Grafana, kafka-exporter, generateur de charge
scripts/start.sh full        # extended + Schema Registry et Kafka Connect
```

Démarre le lab. Choisissez le mode selon la mémoire du poste : `core` demande 4 Go alloués à Docker, `extended` 6 Go. Le premier démarrage prend environ une minute.

```bash
# [hôte]
scripts/validate.sh
scripts/validate.sh | tail -1
```

Contrôle le lab de bout en bout : leader du quorum élu, 3 brokers enregistrés, aucune partition sous-répliquée, aller-retour production et consommation, Kafka UI joignable. La dernière ligne doit être `[ OK ] Environnement valide.` La seconde forme n'affiche que ce verdict.

```bash
# [hôte]
scripts/stop.sh              # arrete les conteneurs, garde les donnees
scripts/stop.sh --down       # supprime les conteneurs, garde les donnees
scripts/reset.sh             # efface tout : topics, groupes, configurations (demande confirmation)
scripts/reset.sh --yes       # idem, sans confirmation
```

`stop.sh` met le lab en pause ; au redémarrage, topics et messages sont toujours là. `reset.sh` remet le lab à neuf : toutes les données sont perdues.

### Voir les conteneurs et entrer dans `tools`

```bash
# [hôte]
docker compose ps
docker compose ps --format 'table {{.Name}}\t{{.Status}}'
```

Liste les conteneurs du lab et leur état. Les nœuds Kafka doivent être `Up ... (healthy)`. `health: starting` signifie que le nœud démarre encore (15 à 20 secondes).

```bash
# [hôte]
docker compose exec tools bash
# [tools]
echo $BS
exit
```

Ouvre le shell d'administration. Le service s'appelle `tools` dans Compose et son conteneur `kafka-tools` : les deux noms fonctionnent. `exit` ramène sur l'hôte.

```bash
# [hôte]
docker compose exec tools kafka-topics.sh --bootstrap-server kafka-1:19092 --list
docker compose exec tools bash -c 'kafka-topics.sh --bootstrap-server $BS --list'
```

Lance une seule commande dans `tools` sans ouvrir de shell. Avec `bash -c '...'` entre apostrophes, `$BS` est lu dans le conteneur. Sans cela, il serait lu sur l'hôte, où il n'existe pas : écrivez alors la liste des brokers en entier.

### Lire les traces d'un conteneur

```bash
# [hôte]
docker compose logs kafka-1 --tail 50
docker compose logs kafka-1 --since 1m
docker compose logs kafka-1 | grep 'Kafka Server started'
```

Affiche ce que le conteneur a écrit : les 50 dernières lignes, la dernière minute, ou seulement les lignes qui contiennent un motif. C'est le premier réflexe quand un nœud ne démarre pas ou reste `unhealthy`.

### Arrêter et redémarrer un nœud

```bash
# [hôte]
docker compose restart kafka-2     # arret propre puis redemarrage
docker compose stop kafka-3        # arret propre
docker compose start kafka-3       # relance un conteneur arrete
docker compose kill kafka-3        # arret brutal, simule une coupure de courant
time docker compose stop kafka-3   # mesure la duree d'un arret propre
```

Un arrêt propre (`stop`, `restart`) laisse le nœud transmettre ses leaderships de partition et quitter le quorum avant de s'éteindre : la bascule est immédiate. Un arrêt brutal (`kill`) ne prévient personne. Le contrôleur attend alors `broker.session.timeout.ms` (9 s par défaut) avant d'écarter le broker. Au redémarrage, Kafka vérifie et reconstruit les index de chaque partition.

```bash
# [hôte]
docker compose exec -d tools bash -c 'kafka-producer-perf-test.sh --bootstrap-server $BS --topic payments \
  --num-records 6000 --throughput 100 --record-size 200 --command-property acks=all \
  > /tmp/perf-payments.log 2>&1'
```

`-d` lance la commande en arrière-plan dans le conteneur et rend la main tout de suite. La sortie part dans un fichier de `tools`, à relire plus tard (`tail /tmp/perf-payments.log` dans `tools`).

### En plus

```bash
# [hôte]
docker compose logs -f kafka-1                            # suit les traces en direct, Ctrl+C pour sortir
docker stats --no-stream kafka-1 kafka-2 kafka-3          # memoire et CPU de chaque noeud
docker compose exec kafka-1 du -sh /var/lib/kafka/data    # place occupee par les donnees du noeud
docker compose exec kafka-1 df -h /var/lib/kafka/data     # place libre sur le disque des donnees
```

`docker stats` montre si un nœud approche de sa limite de mémoire (1 Go par nœud dans le lab). Dans le lab, `df` mesure le disque de la machine virtuelle de Docker, pas un disque dédié.

---

## 2. Installer un nœud KRaft à partir de l'archive (hors lab)

Commandes testées avec l'archive 4.3.2 et Java 21 sur un poste macOS.

```bash
# [serveur]
curl -fsSLO https://downloads.apache.org/kafka/4.3.2/kafka_2.13-4.3.2.tgz
curl -fsSLO https://downloads.apache.org/kafka/4.3.2/kafka_2.13-4.3.2.tgz.sha512
shasum -a 512 kafka_2.13-4.3.2.tgz        # comparer avec le contenu du fichier .sha512
tar -xzf kafka_2.13-4.3.2.tgz && cd kafka_2.13-4.3.2
```

Télécharge l'archive et son empreinte, vérifie que le fichier n'a pas été altéré, puis le décompresse. Les anciennes versions se trouvent sur `archive.apache.org`.

```bash
# [serveur]
KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"
bin/kafka-storage.sh format --standalone -t "$KAFKA_CLUSTER_ID" -c config/server.properties
```

`random-uuid` génère l'identifiant du futur cluster. `format` prépare le répertoire de données : il écrit `meta.properties` (`cluster.id`, `node.id`, `directory.id`) et amorce le journal de métadonnées. `--standalone` crée un quorum d'un seul votant, ce nœud. En KRaft, aucun nœud ne démarre sans avoir été formaté. Un nœud redémarré sur un disque vide ne peut donc pas créer un nouveau cluster par erreur.

```bash
# [serveur]
bin/kafka-server-start.sh -daemon config/server.properties
bin/kafka-metadata-quorum.sh --bootstrap-server localhost:9092 describe --status
bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic test
bin/kafka-server-stop.sh
```

Démarre Kafka en tâche de fond, vérifie que le quorum a un leader, crée un topic de test, puis arrête le nœud.

Dans le lab, le script de démarrage de chaque nœud fait les mêmes gestes : `kafka-storage.sh format --ignore-formatted`, puis lancement de Kafka. `--ignore-formatted` ne touche pas un répertoire déjà formaté, ce qui permet de relancer le script sans perdre les données.

---

## 3. Configuration et identité d'un nœud (TP1)

### Lire `server.properties`

```bash
# [hôte]
grep -E '^(process.roles|node.id|controller.quorum.bootstrap.servers|controller.listener.names|listeners|advertised.listeners|log.dirs)=' \
  config/kafka/kafka-1/server.properties
grep -v '^#' config/kafka/kafka-1/server.properties | grep -v '^$'
```

La première commande extrait les paramètres essentiels, la seconde affiche tout le fichier sans commentaires ni lignes vides. À repérer :

- `process.roles=broker,controller` : le nœud est à la fois broker (il stocke les messages) et contrôleur (il gère les métadonnées) ;
- `node.id` : identifiant unique du nœud dans le cluster ;
- `controller.quorum.bootstrap.servers` : où joindre le quorum des contrôleurs ;
- `listeners` : les interfaces et ports sur lesquels le nœud écoute ;
- `advertised.listeners` : l'adresse que le nœud communique aux clients pour la suite des échanges. Une adresse annoncée fausse laisse passer le premier contact, puis fait échouer les clients ;
- `log.dirs` : le répertoire des données.

### Lire la configuration effective d'un broker

```bash
# [tools]
kafka-configs.sh --bootstrap-server $BS --describe --entity-type brokers --entity-name 1 --all | head -20
kafka-configs.sh --bootstrap-server $BS --describe --entity-type brokers --entity-name 1 --all \
  | grep -E 'min.insync|unclean.leader|default.replication|auto.create'
```

`--all` affiche toutes les valeurs en vigueur, y compris les valeurs par défaut. Le champ `synonyms` indique d'où vient chaque valeur : `STATIC_BROKER_CONFIG` pour `server.properties`, `DYNAMIC_...` pour une modification à chaud, `DEFAULT_CONFIG` pour la valeur par défaut de Kafka. La seconde commande contrôle les réglages de durabilité du cluster. Dans le lab, `auto.create.topics.enable=false` : écrire dans un topic absent échoue au lieu de le créer.

### Vérifier le formatage

```bash
# [hôte]
docker compose exec kafka-1 /opt/kafka/bin/kafka-storage.sh info -c /opt/lab/node/server.properties
docker compose exec kafka-1 cat /var/lib/kafka/data/meta.properties
docker compose logs kafka-1 | grep -E 'start-node|Formatting|already formatted|Kafka Server started'
```

`kafka-storage.sh info` et `meta.properties` montrent l'identité du nœud : à quel cluster il appartient (`cluster.id`), son numéro (`node.id`) et l'identifiant de son répertoire de données (`directory.id`). Dans les traces, le premier démarrage affiche `Formatting ...` et les suivants `All of the log directories are already formatted.`

### Lire les fichiers de traces de Kafka

```bash
# [hôte]
docker compose exec kafka-1 ls /opt/kafka/logs
docker compose exec kafka-1 grep 'Kafka Server started' /opt/kafka/logs/server.log
```

Kafka écrit plusieurs fichiers : `server.log` (journal général), `controller.log` (contrôleur), `state-change.log` (changements de leader), `kafka-request.log`, `kafka-authorizer.log`, `log-cleaner.log` et le journal du ramasse-miettes Java. Les fichiers tournent toutes les heures : `controller.log.2026-10-27-21`, etc.

```bash
# [hôte]
for n in 1 2 3; do
  echo "kafka-$n : $(docker compose exec -T kafka-$n sh -c 'grep -h "Becoming the active controller" /opt/kafka/logs/controller.log*')"
done
```

Cherche sur les 3 nœuds la trace de l'élection du contrôleur actif. `controller.log*` inclut les fichiers déjà tournés. `-T` empêche Docker d'ouvrir un terminal, ce qui est nécessaire dans une boucle.

### Décoder le journal de métadonnées

```bash
# [hôte]
docker compose exec kafka-1 ls /var/lib/kafka/data/__cluster_metadata-0
docker compose exec kafka-1 sh -c '/opt/kafka/bin/kafka-dump-log.sh --cluster-metadata-decoder \
  --files /var/lib/kafka/data/__cluster_metadata-0/00000000000000000000.log 2>/dev/null \
  | grep -o "\"type\":\"[A-Z_]*\"" | sort | uniq -c | sort -rn'
docker compose exec kafka-1 sh -c '/opt/kafka/bin/kafka-dump-log.sh --cluster-metadata-decoder \
  --files /var/lib/kafka/data/__cluster_metadata-0/00000000000000000000.log 2>/dev/null \
  | grep -o "payload: {\"type\":\"TOPIC_RECORD\".*" | head -3'
```

En KRaft, l'état du cluster (topics, partitions, brokers, configurations) est un journal, la partition `__cluster_metadata-0`. `kafka-dump-log.sh --cluster-metadata-decoder` le rend lisible. La deuxième commande compte les enregistrements par type, la troisième montre la création des topics (`TOPIC_RECORD`). Le fichier `quorum-state` garde l'état local du vote, le `.checkpoint` est un instantané des métadonnées.

### En plus

```bash
# [tools]
kafka-topics.sh --version
kafka-consumer-groups.sh --help | less
```

`--version` donne la version des outils (4.3.2 dans le lab). `--help | less` permet de parcourir l'aide page par page (`q` pour sortir).

---

## 4. Quorum KRaft et état du cluster (TP1)

### Le quorum

```bash
# [tools]
kafka-metadata-quorum.sh --bootstrap-server $BS describe --status
kafka-metadata-quorum.sh --bootstrap-server $BS describe --replication
```

`--status` résume le quorum :

- `LeaderId` : le contrôleur actif ;
- `LeaderEpoch` : augmente à chaque élection. Une époque qui grimpe sur un cluster stable signale des élections à répétition ;
- `HighWatermark` : jusqu'où le journal de métadonnées est validé par la majorité ;
- `CurrentVoters` : les contrôleurs qui votent ;
- `CurrentObservers` : les brokers seuls, qui suivent le journal sans voter.

`--replication` donne une ligne par nœud, avec son rôle (`Leader`, `Follower`, `Observer`) et son retard (`Lag`), qui doit rester à 0 ou presque.

### Les brokers et le cluster

```bash
# [tools]
kafka-broker-api-versions.sh --bootstrap-server $BS | grep '^kafka'
kafka-cluster.sh cluster-id --bootstrap-server $BS
kafka-cluster.sh list-endpoints --bootstrap-server $BS
```

- `kafka-broker-api-versions.sh` liste les brokers enregistrés, leur rack et `isFenced`. `isFenced: false` veut dire que le contrôleur accepte le broker.
- `cluster-id` affiche l'identifiant du cluster.
- `list-endpoints` donne pour chaque broker son adresse, son rack et son état (`unfenced` = actif).

### Les fonctionnalités activées

```bash
# [tools]
kafka-features.sh --bootstrap-server $BS describe
kafka-features.sh --bootstrap-server $BS version-mapping --release-version 4.3
```

`describe` affiche les niveaux de fonctionnalités en vigueur. `metadata.version` (4.3-IV0) fixe le format des métadonnées et pilote les montées de version. `kraft.version=1` indique un quorum dynamique, dont on peut ajouter ou retirer des votants. `version-mapping` montre les niveaux associés à une version de Kafka.

### Kafka UI

```bash
# [hôte]
curl -s localhost:8080/api/clusters
```

Interroge l'API de Kafka UI : le cluster `novashop-lab` doit être `ONLINE` avec 3 brokers. Dans le navigateur, http://localhost:8080, menus « Brokers » et « Topics ».

### Perte de quorum (démonstration)

```bash
# [hôte]
docker compose stop kafka-2 kafka-3
# [tools]
timeout 45 kafka-metadata-quorum.sh --bootstrap-server kafka-1:19092 describe --status; echo "code retour : $?"
kafka-topics.sh --bootstrap-server kafka-1:19092 --list | head -5
timeout 45 kafka-topics.sh --bootstrap-server kafka-1:19092 --create --topic quorum-test; echo "code retour : $?"
# [hôte]
docker compose logs --since 60s kafka-1 | grep -oE 'transition to [A-Za-z]+State' | sort | uniq -c
docker compose start kafka-2 kafka-3
```

Avec 2 nœuds arrêtés sur 3, il n'y a plus de majorité : plus de contrôleur actif. `timeout 45` coupe une commande au bout de 45 secondes (code retour 124). La lecture des métadonnées fonctionne encore (`--list`, servi depuis la copie locale du broker). Toute modification est impossible (`--create` bloque). Les traces montrent `kafka-1` qui tente en boucle une élection sans majorité. Le cluster se rétablit dès que les nœuds reviennent.

---

## 5. Topics (TP2)

### Créer, lister, décrire

```bash
# [tools]
kafka-topics.sh --bootstrap-server $BS --list
kafka-topics.sh --bootstrap-server $BS --create --topic orders --partitions 3 --replication-factor 3
kafka-topics.sh --bootstrap-server $BS --create --if-not-exists --topic orders --partitions 3 --replication-factor 3
kafka-topics.sh --bootstrap-server $BS --describe --topic orders
```

- `--partitions` fixe le parallélisme du topic, `--replication-factor` le nombre de copies de chaque partition.
- `--if-not-exists` ne signale pas d'erreur si le topic existe déjà : pratique dans un script.
- `--describe` donne, par partition, le `Leader` (le broker qui reçoit les écritures), les `Replicas` (les brokers qui portent une copie, le premier étant le leader préféré) et l'`Isr` (les copies à jour).

Un facteur de réplication supérieur au nombre de brokers est refusé : `--replication-factor 4` échoue sur 3 brokers.

### Modifier, supprimer

```bash
# [tools]
kafka-topics.sh --bootstrap-server $BS --alter --topic orders --partitions 6
kafka-topics.sh --bootstrap-server $BS --delete --topic tmp-test
```

On peut augmenter le nombre de partitions, jamais le réduire (`3 would not be an increase`). Attention : la partition d'un message avec clé se calcule modulo le nombre de partitions. Après l'ajout, les nouveaux messages d'une même clé peuvent arriver dans une autre partition que les anciens. `--delete` n'affiche rien en cas de succès ; la suppression des fichiers est asynchrone.

### Imposer le placement des répliques

```bash
# [tools]
kafka-topics.sh --bootstrap-server $BS --create --topic returns --replica-assignment 1:2,2:3,3:1
```

Place la partition 0 sur les brokers 1 et 2, la 1 sur 2 et 3, la 2 sur 3 et 1. Les partitions sont séparées par des virgules, les brokers d'une partition par des deux-points. Ne pas donner `--partitions` ni `--replication-factor` en même temps.

### Configurer un topic

```bash
# [tools]
kafka-topics.sh --bootstrap-server $BS --create --topic clickstream --partitions 6 --replication-factor 3
kafka-configs.sh --bootstrap-server $BS --alter --entity-type topics --entity-name clickstream \
  --add-config retention.ms=86400000,retention.bytes=104857600
kafka-configs.sh --bootstrap-server $BS --describe --entity-type topics --entity-name clickstream
kafka-configs.sh --bootstrap-server $BS --describe --entity-type topics --entity-name clickstream --all
kafka-configs.sh --bootstrap-server $BS --alter --entity-type topics --entity-name clickstream \
  --delete-config retention.bytes
kafka-topics.sh --bootstrap-server $BS --describe --topics-with-overrides
```

- `--add-config` modifie un réglage à chaud, sans redémarrage. Ici : garder 1 jour de messages, et au plus 100 Mo par partition.
- `--describe` montre les réglages propres au topic ; avec `--all`, toutes les valeurs effectives et leur origine.
- `--delete-config` supprime le réglage propre au topic : la valeur du broker s'applique de nouveau.
- `--topics-with-overrides` liste les topics qui ont au moins un réglage propre.

On peut aussi régler un topic dès sa création : `--config retention.ms=60000`.

### En plus

```bash
# [tools]
kafka-topics.sh --bootstrap-server $BS --list --exclude-internal
kafka-log-dirs.sh --bootstrap-server $BS --describe --topic-list orders
```

`--exclude-internal` masque les topics internes de Kafka (`__consumer_offsets`). Les topics des composants de l'écosystème (`_schemas`, `_connect-*`) restent affichés. `kafka-log-dirs.sh` donne, pour chaque broker et chaque partition, la taille sur disque en octets (`size`) et le retard d'une copie en cours de déplacement (`offsetLag`). La sortie est en JSON.

---

## 6. Produire et consommer (TP2 à TP4)

### Produire

```bash
# [tools]
echo "test" | kafka-console-producer.sh --bootstrap-server $BS --topic orders \
  --command-property max.block.ms=5000
```

Envoie un message. Si le topic n'existe pas, le producteur attend ses métadonnées pendant `max.block.ms`, ici 5 s au lieu de 60, puis échoue avec `Topic orders not present in metadata`.

```bash
# [tools]
head -3 /opt/lab/data/orders.txt
kafka-console-producer.sh --bootstrap-server $BS --topic orders \
  --reader-property parse.key=true --reader-property 'key.separator=|' \
  < /opt/lab/data/orders.txt
```

Produit des messages avec clé. Chaque ligne du fichier a la forme `clé|valeur`. `parse.key=true` demande de découper la ligne, `key.separator` donne le séparateur. Tous les messages d'une même clé (ici un client) vont dans la même partition : leur ordre est garanti.

```bash
# [tools]
seq 1 1000 | sed 's/^/click-/' | kafka-console-producer.sh --bootstrap-server $BS --topic clickstream
```

Produit 1000 messages sans clé (`click-1` à `click-1000`). Sans clé, le producteur remplit un lot dans une partition, puis passe à une autre : sur un envoi court, tout peut arriver dans peu de partitions.

```bash
# [tools]
seq 1 100 | sed 's/^/PAY-/' | kafka-console-producer.sh --bootstrap-server $BS --topic payments \
  --request-required-acks all
echo REF-2 | kafka-console-producer.sh --bootstrap-server $BS --topic refunds --request-required-acks 1
echo REF-1 | kafka-console-producer.sh --bootstrap-server $BS --topic refunds --request-required-acks all \
  --command-property delivery.timeout.ms=15000 --command-property request.timeout.ms=5000
```

- `--request-required-acks all` : le leader ne confirme qu'une fois le message copié sur toutes les répliques de l'ISR, et au moins `min.insync.replicas` copies.
- `--request-required-acks 1` : le leader confirme seul, dès l'écriture locale.
- `delivery.timeout.ms` et `request.timeout.ms` raccourcissent l'attente avant l'échec (2 minutes par défaut pour la livraison), pratique pour voir une erreur sans attendre.

### Consommer

```bash
# [tools]
kafka-console-consumer.sh --bootstrap-server $BS --topic orders --from-beginning --max-messages 10
kafka-console-consumer.sh --bootstrap-server $BS --topic orders --partition 0 --offset 0 --max-messages 5 \
  --formatter-property print.key=true --formatter-property print.partition=true \
  --formatter-property print.offset=true
kafka-console-consumer.sh --bootstrap-server $BS --topic orders --from-beginning --timeout-ms 5000 2>/dev/null | wc -l
```

- `--from-beginning` lit depuis le début du topic ; sans cette option, seuls les nouveaux messages s'affichent.
- `--partition` et `--offset` lisent une partition précise à partir d'un offset donné.
- `--max-messages` s'arrête après N messages, `--timeout-ms` après N millisecondes sans nouveau message.
- Les `--formatter-property print.xxx=true` ajoutent la partition, l'offset et la clé devant chaque valeur.
- `2>/dev/null | wc -l` masque les messages techniques et compte les messages lus.

```bash
# [tools]
kafka-console-consumer.sh --bootstrap-server $BS --topic orders --from-beginning --max-messages 30 \
  --formatter-property print.key=true --formatter-property print.partition=true 2>/dev/null \
  | awk '{print $2, $1}' | sort -u
```

Affiche une ligne par couple (client, partition), par exemple `C002 Partition:0`. Avant l'ajout de partitions, chaque client n'apparaît que dans une partition. Kafka calcule la partition par `murmur2(clé) modulo nombre de partitions`.

### Lire les offsets

```bash
# [tools]
kafka-get-offsets.sh --bootstrap-server $BS --topic orders
kafka-get-offsets.sh --bootstrap-server $BS --topic orders --time earliest
kafka-get-offsets.sh --bootstrap-server $BS --topic payments | awk -F: '{s += $3} END {print "payments :", s}'
```

Affiche `topic:partition:offset`. Par défaut (`latest`), l'offset de fin, soit le prochain offset qui sera écrit. Avec `--time earliest`, le premier offset encore disponible : il avance quand la rétention supprime des messages. Sur un topic jamais purgé, la troisième commande donne le nombre total de messages.

### Mesurer un débit

```bash
# [tools]
kafka-producer-perf-test.sh --bootstrap-server $BS --topic payments \
  --num-records 6000 --throughput 100 --record-size 200 --command-property acks=all
```

Envoie 6000 messages de 200 octets à 100 messages par seconde, puis affiche le débit et les latences. Il sert à générer de la charge pendant une panne, ou à comparer des réglages (`acks`, `linger.ms`, `compression.type`).

### En plus

```bash
# [tools]
kafka-console-consumer.sh --bootstrap-server $BS --topic orders --from-beginning --max-messages 5 \
  --formatter-property print.timestamp=true --formatter-property print.headers=true
kafka-get-offsets.sh --bootstrap-server $BS --topic orders --time max-timestamp
kafka-get-offsets.sh --bootstrap-server $BS --topic orders --time $(( $(date +%s) * 1000 - 3600000 ))
```

- `print.timestamp` affiche l'horodatage de chaque message (`CreateTime:` suivi de millisecondes depuis 1970), `print.headers` ses en-têtes (`NO_HEADERS` s'il n'y en a pas).
- `--time max-timestamp` donne l'offset du message à l'horodatage le plus récent.
- `--time` suivi d'un horodatage en millisecondes donne, par partition, le premier offset écrit à partir de cet instant : ici, il y a une heure. Utile pour savoir où recommencer une lecture après un incident.

---

## 7. Consumer groups et offsets (TP3)

### Observer un groupe

```bash
# [tools]
kafka-console-consumer.sh --bootstrap-server $BS --topic orders --group billing \
  --from-beginning --timeout-ms 10000 | wc -l
kafka-consumer-groups.sh --bootstrap-server $BS --list
kafka-consumer-groups.sh --bootstrap-server $BS --describe --group billing
```

`--group` fait consommer au nom d'un groupe : le groupe enregistre sa position (son offset validé) dans `__consumer_offsets` et reprend là où il s'est arrêté. `--describe` affiche par partition :

- `CURRENT-OFFSET` : la position du groupe ;
- `LOG-END-OFFSET` : la fin de la partition ;
- `LAG` : la différence, soit ce qui reste à lire ;
- `CONSUMER-ID` : le consommateur qui lit la partition, vide si personne ne la lit.

```bash
# [tools]
kafka-consumer-groups.sh --bootstrap-server $BS --describe --group analytics --members --verbose
kafka-consumer-groups.sh --bootstrap-server $BS --describe --group analytics --state
kafka-consumer-groups.sh --bootstrap-server $BS --list --type
```

- `--members --verbose` montre chaque membre et ses partitions.
- `--state` montre le coordinateur du groupe (le broker qui le gère), la stratégie d'affectation, l'état (`Stable`, `Empty`, ou en cours de rééquilibrage) et le nombre de membres.
- `--list --type` indique pour chaque groupe son protocole : `Classic` ou `Consumer` (nouveau protocole).

Lancez le même consommateur dans deux terminaux avec le même `--group` : les partitions se répartissent entre eux. Arrêtez-en un (`Ctrl+C`) : ses partitions passent à l'autre, c'est un rééquilibrage.

### Le nouveau protocole et les membres statiques

```bash
# [tools]
kafka-console-consumer.sh --bootstrap-server $BS --topic orders --group analytics-v2 \
  --command-property group.protocol=consumer
kafka-console-consumer.sh --bootstrap-server $BS --topic orders --group statique \
  --consumer-property group.instance.id=inst-1 \
  --consumer-property partition.assignment.strategy=org.apache.kafka.clients.consumer.CooperativeStickyAssignor
```

- `group.protocol=consumer` utilise le protocole de groupe de Kafka 4 (KIP-848) : c'est le broker qui calcule l'affectation, et un départ ne déplace que les partitions du membre parti.
- `group.instance.id` fait du consommateur un membre statique : s'il redémarre dans le délai `session.timeout.ms` (45 s par défaut), il retrouve ses partitions sans rééquilibrage.

### Déplacer la position d'un groupe

Le groupe doit être inactif : arrêtez d'abord tous ses consommateurs. Sinon, l'outil répond `Assignments can only be reset if the group 'analytics' is inactive`.

```bash
# [tools]
kafka-consumer-groups.sh --bootstrap-server $BS --group billing --topic orders \
  --reset-offsets --to-earliest --dry-run
kafka-consumer-groups.sh --bootstrap-server $BS --group billing --topic orders \
  --reset-offsets --to-earliest --execute
```

`--dry-run` affiche les nouveaux offsets sans rien changer. `--execute` les applique. Lancez toujours `--dry-run` d'abord.

```bash
# [tools]
kafka-consumer-groups.sh --bootstrap-server $BS --group billing --topic orders --reset-offsets --to-latest --execute
kafka-consumer-groups.sh --bootstrap-server $BS --group billing --topic orders --reset-offsets --shift-by -5 --execute
kafka-consumer-groups.sh --bootstrap-server $BS --group billing --topic orders --reset-offsets --by-duration PT1H --execute
kafka-consumer-groups.sh --bootstrap-server $BS --group billing --topic orders \
  --reset-offsets --to-datetime 2026-10-27T13:00:00.000 --execute
```

- `--to-earliest` : relire tout ce qui est encore disponible.
- `--to-latest` : sauter à la fin, en ignorant le retard accumulé.
- `--shift-by -5` : reculer de 5 messages dans chaque partition (un nombre positif avance).
- `--by-duration PT1H` : revenir une heure en arrière (format de durée ISO 8601).
- `--to-datetime` : revenir à une date précise. Les conteneurs sont à l'heure UTC.

```bash
# [tools]
kafka-consumer-groups.sh --bootstrap-server $BS --group billing --topic orders \
  --reset-offsets --to-current --export --dry-run > /tmp/billing-offsets.csv
cat /tmp/billing-offsets.csv
kafka-consumer-groups.sh --bootstrap-server $BS --group billing --reset-offsets --from-file /tmp/billing-offsets.csv --dry-run
kafka-consumer-groups.sh --bootstrap-server $BS --group billing --reset-offsets --from-file /tmp/billing-offsets.csv --execute
```

Sauvegarde les positions actuelles du groupe dans un fichier CSV, puis les restaure. C'est le filet de sécurité à prendre avant toute réinitialisation.

### En plus

```bash
# [tools]
kafka-consumer-groups.sh --bootstrap-server $BS --describe --all-groups --state
kafka-consumer-groups.sh --bootstrap-server $BS --group billing --topic orders:0 \
  --reset-offsets --to-offset 15 --dry-run
kafka-consumer-groups.sh --bootstrap-server $BS --delete-offsets --group old-app --topic orders
kafka-consumer-groups.sh --bootstrap-server $BS --delete --group old-app
```

- `--all-groups` décrit tous les groupes d'un coup.
- `--topic orders:0` limite l'action à la partition 0. `--to-offset 15` place le groupe à un offset précis, par exemple pour sauter un message illisible.
- `--delete-offsets` efface les positions d'un groupe sur un topic, sans supprimer le groupe : utile quand une application ne lit plus ce topic. `old-app` désigne ici un groupe qui ne sert plus ; ne supprimez pas `billing`, réutilisé plus tard.
- `--delete` supprime un groupe inactif et toutes ses positions.

---

## 8. Rétention et compaction (TP3)

### Rétention dans le temps

```bash
# [tools]
kafka-topics.sh --bootstrap-server $BS --create --topic audit-short --partitions 1 --replication-factor 3 \
  --config retention.ms=60000 --config segment.ms=10000
seq 1 100 | sed 's/^/audit-/' | kafka-console-producer.sh --bootstrap-server $BS --topic audit-short
sleep 35; echo audit-101 | kafka-console-producer.sh --bootstrap-server $BS --topic audit-short
for i in $(seq 1 12); do
  echo "$(date +%T) $(kafka-get-offsets.sh --bootstrap-server $BS --topic audit-short --time earliest)"
  sleep 10
done
```

`retention.ms=60000` garde les messages une minute. Kafka supprime des segments entiers, et jamais le segment actif, celui en cours d'écriture. `segment.ms=10000` ferme donc un segment toutes les 10 secondes, et le message envoyé après 35 secondes en ouvre un nouveau. La boucle affiche le premier offset disponible toutes les 10 secondes : il saute quand le vieux segment est supprimé.

```bash
# [hôte]
docker compose exec kafka-1 ls -l /var/lib/kafka/data/audit-short-0
```

Montre les fichiers d'une partition sur disque. Chaque segment a son `.log` (les messages), son `.index` (offset vers position dans le fichier) et son `.timeindex` (horodatage vers offset). Le nom du fichier est le premier offset du segment.

### Compaction

```bash
# [tools]
kafka-topics.sh --bootstrap-server $BS --create --topic product-prices --partitions 1 --replication-factor 3 \
  --config cleanup.policy=compact --config segment.ms=10000 \
  --config min.cleanable.dirty.ratio=0.01 --config delete.retention.ms=1000
kafka-console-producer.sh --bootstrap-server $BS --topic product-prices \
  --reader-property parse.key=true --reader-property 'key.separator=|' < /opt/lab/data/product-prices.txt
kafka-console-consumer.sh --bootstrap-server $BS --topic product-prices --from-beginning --timeout-ms 5000 \
  --formatter-property print.key=true --formatter-property print.offset=true
```

- `cleanup.policy=compact` : au lieu de supprimer selon l'âge, Kafka garde la dernière valeur de chaque clé. Les anciennes versions disparaissent quand le nettoyeur passe ; les offsets restants ne se suivent alors plus.
- `min.cleanable.dirty.ratio=0.01` : le nettoyeur passe dès que 1 % du journal contient des valeurs périmées (50 % par défaut).
- `delete.retention.ms` : durée de conservation des tombstones (voir ci-dessous) après le nettoyage.

```bash
# [tools]
echo 'P-500|NULL' | kafka-console-producer.sh --bootstrap-server $BS --topic product-prices \
  --reader-property parse.key=true --reader-property 'key.separator=|' --reader-property null.marker=NULL
```

Envoie un tombstone : un message avec une clé et une valeur nulle. `null.marker=NULL` transforme le texte `NULL` en vraie valeur nulle. Après compaction, la clé `P-500` disparaît complètement du topic. C'est ainsi qu'on supprime une clé d'un topic compacté.

### En plus

Supprimer des messages sans attendre la rétention, sur un topic jetable :

```bash
# [tools]
kafka-topics.sh --bootstrap-server $BS --create --topic delete-test --partitions 1 --replication-factor 3
seq 1 20 | sed 's/^/m-/' | kafka-console-producer.sh --bootstrap-server $BS --topic delete-test
echo '{"partitions":[{"topic":"delete-test","partition":0,"offset":10}],"version":1}' > /tmp/delete.json
kafka-delete-records.sh --bootstrap-server $BS --offset-json-file /tmp/delete.json
kafka-get-offsets.sh --bootstrap-server $BS --topic delete-test --time earliest
```

```bash
# [hôte]
docker compose exec kafka-1 sh -c '/opt/kafka/bin/kafka-dump-log.sh --print-data-log \
  --files /var/lib/kafka/data/delete-test-0/00000000000000000000.log | head -5'
```

```bash
# [tools]
kafka-topics.sh --bootstrap-server $BS --delete --topic delete-test
```

- `kafka-delete-records.sh` supprime tous les messages avant l'offset indiqué (ici 10). Le premier offset disponible passe aussitôt à 10. Opération irréversible.
- `kafka-dump-log.sh --print-data-log` lit un segment directement sur disque : offset, horodatage, taille de la clé et contenu de chaque message. Sur le lab, les offsets 0 à 9 figuraient encore dans le segment juste après la suppression : ils ne sont plus lisibles par les consommateurs, mais le fichier n'est effacé que plus tard, avec son segment.

---

## 9. Réplication, ISR et panne d'un broker (TP4)

### Régler le minimum ISR

```bash
# [tools]
kafka-configs.sh --bootstrap-server $BS --describe --entity-type brokers --entity-default
kafka-configs.sh --bootstrap-server $BS --alter --entity-type brokers --entity-default \
  --add-config min.insync.replicas=2
kafka-topics.sh --bootstrap-server $BS --create --topic refunds --partitions 1 --replication-factor 3 \
  --config min.insync.replicas=3
kafka-configs.sh --bootstrap-server $BS --describe --entity-type topics --entity-name payments --all \
  | grep min.insync
```

`min.insync.replicas` est le nombre minimal de copies à jour pour qu'une écriture en `acks=all` soit acceptée. `--entity-default` le fixe pour tout le cluster ; `--config` à la création le fixe pour un seul topic. En Kafka 4.3, avec l'ELR (eligible leader replicas) activé, une valeur par broker (`--entity-name 1`) est refusée : `Broker-level min.insync.replicas cannot be altered while ELR is enabled`.

### Diagnostiquer une panne

```bash
# [tools]
kafka-topics.sh --bootstrap-server $BS --describe --under-replicated-partitions
kafka-topics.sh --bootstrap-server $BS --describe --under-replicated-partitions | grep -c Partition
kafka-topics.sh --bootstrap-server $BS --describe --at-min-isr-partitions | grep -c Partition
kafka-topics.sh --bootstrap-server $BS --describe --under-min-isr-partitions
kafka-topics.sh --bootstrap-server $BS --describe --topic payments
```

- `--under-replicated-partitions` (URP) : partitions dont au moins une copie n'est plus à jour. Ce doit être 0 sur un cluster sain.
- `--at-min-isr-partitions` : partitions qui ont tout juste le minimum de copies à jour. Une panne de plus et les écritures `acks=all` y échouent.
- `--under-min-isr-partitions` : partitions sous le minimum. Les écritures `acks=all` y sont refusées (`NotEnoughReplicas`).
- `grep -c Partition` compte les lignes de partition.

Pendant la panne de `kafka-3`, les avertissements `Couldn't resolve server kafka-3:19092` sont normaux : `$BS` contient encore l'adresse du nœud arrêté.

Ce qui se passe avec 2 brokers sur 3 :

- `payments` (`min.insync.replicas=2`) accepte toujours les écritures `acks=all` : 2 copies à jour, c'est assez ;
- `refunds` (`min.insync.replicas=3`) les refuse ;
- `refunds` accepte encore les écritures `acks=1`, mais elles ne sont pas protégées par la réplication.

### Revenir à la normale

```bash
# [hôte]
docker compose start kafka-3
```

```bash
# [tools]
while [ "$(kafka-topics.sh --bootstrap-server $BS --describe --under-replicated-partitions 2>/dev/null | grep -c Partition)" != 0 ]; do
  date +%T; sleep 2
done; echo "URP = 0 a $(date +%T)"
kafka-leader-election.sh --bootstrap-server $BS --election-type preferred --all-topic-partitions
kafka-topics.sh --bootstrap-server $BS --describe --topic payments
```

La boucle attend que toutes les copies aient rattrapé leur retard. Au retour d'un broker, ses partitions gardent leur leader temporaire. L'élection préférée rend le leadership au premier broker de la liste `Replicas`, ce qui rééquilibre la charge. Kafka le fait aussi seul (`auto.leader.rebalance.enable=true`) : il contrôle l'équilibre toutes les 5 minutes.

```bash
# [tools]
kafka-leader-election.sh --bootstrap-server $BS --election-type unclean --topic app-logs --partition 0
```

Exemple de syntaxe : le topic `app-logs` n'existe pas dans le lab. L'élection unclean donne le leadership à une copie qui n'est pas à jour, quand aucune copie à jour ne survit. La partition redevient disponible, mais les messages que cette copie n'avait pas reçus sont perdus. À réserver au dernier recours.

### Lire les alertes et les traces

```bash
# [hôte]
curl -s localhost:9090/api/v1/alerts | grep -o '"alertname":"[A-Za-z]*"' | sort | uniq -c
docker compose logs kafka-3 --since 2m | grep -E 'clean shutdown file'
```

La première commande liste les alertes Prometheus actives (mode `extended`), également visibles sur http://localhost:9090/alerts. La seconde montre comment `kafka-3` s'était arrêté. `Skipping recovery ... since clean shutdown file was found` : l'arrêt était propre. `Recovering ... since no clean shutdown file was found` : l'arrêt était brutal, et Kafka a vérifié chaque partition.

### En plus

```bash
# [tools]
kafka-topics.sh --bootstrap-server $BS --describe --unavailable-partitions
kafka-topics.sh --bootstrap-server $BS --describe | grep -E 'ReplicationFactor: [12]\b'
kafka-configs.sh --bootstrap-server $BS --describe --entity-type brokers --entity-name 1 --all | grep broker.rack
```

- `--unavailable-partitions` liste les partitions sans leader, donc ni lisibles ni inscriptibles. Ce doit être vide.
- La deuxième commande repère les topics à 1 ou 2 copies, trop fragiles pour des données importantes.
- `broker.rack` donne la zone du broker (`az-a`, `az-b`, `az-c` dans le lab). Kafka place les copies d'une partition dans des racks différents.

---

## 10. Transactions (démonstration)

```bash
# [tools]
kafka-topics.sh --bootstrap-server $BS --create --topic tx-demo --partitions 1 --replication-factor 3
kafka-producer-perf-test.sh --bootstrap-server $BS --topic tx-demo --num-records 600 --throughput 10 \
  --record-size 50 --transactional-id novashop-tx --transaction-duration-ms 50000 > /tmp/tx.log 2>&1 &
sleep 12
kafka-console-consumer.sh --bootstrap-server $BS --topic tx-demo --from-beginning --max-messages 50 \
  --isolation-level read_uncommitted 2>/dev/null | wc -l
kafka-console-consumer.sh --bootstrap-server $BS --topic tx-demo --from-beginning --timeout-ms 5000 \
  --isolation-level read_committed 2>/dev/null | wc -l
kafka-transactions.sh --bootstrap-server $BS describe --transactional-id novashop-tx
wait
kafka-topics.sh --bootstrap-server $BS --delete --topic tx-demo
```

- Le producteur envoie ses messages dans des transactions de 50 secondes, en arrière-plan (`&`).
- Pendant une transaction ouverte, un consommateur `read_uncommitted` voit déjà les messages, un consommateur `read_committed` n'en voit aucun.
- `kafka-transactions.sh describe` montre l'état de la transaction (`Ongoing`, puis `CompleteCommit`) et son délai maximal (`transaction.timeout.ms`, 60 s par défaut).
- `wait` attend la fin du producteur.

### En plus

```bash
# [tools]
kafka-transactions.sh --bootstrap-server $BS list
kafka-transactions.sh --bootstrap-server $BS find-hanging --broker-id 1
```

`list` affiche tous les identifiants transactionnels connus et leur état. `find-hanging` cherche, sur un broker, les transactions restées ouvertes sans producteur. Une telle transaction bloque les consommateurs `read_committed` de la partition.

---

## 11. Schema Registry et Kafka Connect (démonstration, mode `full`)

### Schema Registry

```bash
# [hôte]
curl -s localhost:8081/subjects
curl -s localhost:8081/subjects/orders-avro-value/versions
curl -s localhost:8081/subjects/orders-avro-value/versions/latest
curl -s localhost:8081/config
```

Interroge l'API REST du registre : les sujets (un par topic et par clé ou valeur), les versions d'un schéma, la dernière version, et la règle de compatibilité globale (`BACKWARD` par défaut).

```bash
# [hôte]
CT='Content-Type: application/vnd.schemaregistry.v1+json'
V2_SANS_DEFAUT='{"schema":"{\"type\":\"record\",\"name\":\"Order\",\"namespace\":\"com.novashop\",\"fields\":[{\"name\":\"order_id\",\"type\":\"string\"},{\"name\":\"amount\",\"type\":\"double\"},{\"name\":\"currency\",\"type\":\"string\"}]}"}'
curl -s -X POST -H "$CT" 'localhost:8081/compatibility/subjects/orders-avro-value/versions/latest?verbose=true' -d "$V2_SANS_DEFAUT"
```

Teste un nouveau schéma sans l'enregistrer. `V2_SANS_DEFAUT` ajoute un champ `currency` sans valeur par défaut au schéma `Order` de la démonstration. Il est jugé incompatible : avec la règle `BACKWARD`, un nouveau champ doit avoir une valeur par défaut, sinon le nouveau schéma ne sait pas lire les anciens messages.

```bash
# [hôte]
docker compose exec tools bash -c 'kafka-console-consumer.sh --bootstrap-server $BS --topic orders-avro --from-beginning --max-messages 2 2>/dev/null | od -c | head -3'
docker compose exec tools bash -c 'kafka-topics.sh --bootstrap-server $BS --describe --topic _schemas | head -1'
```

La première commande montre les octets bruts d'un message Avro : un octet magique, l'identifiant du schéma sur 4 octets, puis les données. Le schéma lui-même n'est pas dans le message. La seconde décrit `_schemas`, le topic compacté où le registre stocke ses schémas.

### Kafka Connect

```bash
# [hôte]
curl -s localhost:8083/connector-plugins
curl -s 'localhost:8083/connectors?expand=status'
curl -s localhost:8083/connectors/clickstream-file-sink/status
curl -s -X POST 'localhost:8083/connectors/clickstream-file-sink/restart?includeTasks=true&onlyFailed=true'
curl -s -X DELETE localhost:8083/connectors/clickstream-file-sink
```

- `connector-plugins` liste les connecteurs installés.
- `connectors?expand=status` donne l'état de chaque connecteur et de ses tâches. Un connecteur `RUNNING` peut cacher une tâche `FAILED` : regardez toujours les tâches.
- `restart` relance les tâches en échec, `DELETE` supprime un connecteur.

```bash
# [hôte]
curl -s -X PUT -H 'Content-Type: application/json' localhost:8083/connectors/clickstream-file-source/config \
  -d '{"connector.class":"org.apache.kafka.connect.file.FileStreamSourceConnector","tasks.max":"1","file":"/opt/lab/data/clickstream-source.txt","topic":"clickstream-raw"}'
```

Crée ou met à jour un connecteur. Ici, une source qui lit un fichier ligne à ligne et l'écrit dans `clickstream-raw`. `PUT .../config` fait les deux : il crée le connecteur s'il n'existe pas, sinon il remplace sa configuration.

```bash
# [hôte]
docker compose exec tools bash -c 'kafka-topics.sh --bootstrap-server $BS --list | grep connect; kafka-consumer-groups.sh --bootstrap-server $BS --list | grep connect'
docker compose stop schema-registry connect
```

Connect stocke ses configurations, offsets et états dans trois topics internes (`_connect-configs`, `_connect-offsets`, `_connect-status`). Un connecteur puits consomme avec un groupe `connect-<nom>`, que l'on surveille comme les autres. La dernière commande libère environ 1 Go de mémoire quand la démonstration est finie.

---

## 12. Lab ou production

Plusieurs réglages du lab servent la démonstration et ne conviennent pas à la production :

- **Sécurité** : `$BS` utilise un listener PLAINTEXT, sans chiffrement ni authentification, et Kafka UI est ouvert à tous. En production, les clients passent par SASL_SSL (jour 2).
- **Nœuds combinés** : chaque nœud est à la fois broker et contrôleur. En production, on utilise des contrôleurs dédiés, pour qu'une surcharge d'un broker ne ralentisse pas tout le cluster.
- **Rétention accélérée** : le lab vérifie la rétention toutes les 30 s (`log.retention.check.interval.ms=30000`, 5 minutes par défaut).
- **Rééquilibrage immédiat** : le lab règle `group.initial.rebalance.delay.ms=0`, contre 3 s par défaut, pour que les groupes se forment sans attendre.
- **Réglages de TP** : `segment.ms=10000`, `min.cleanable.dirty.ratio=0.01`, `delete.retention.ms=1000` et `retention.ms=60000` montrent la rétention et la compaction en quelques minutes. En production, ils créeraient des milliers de petits segments et un nettoyage permanent.
- **Commandes sans retour arrière** : `--reset-offsets --execute`, `kafka-delete-records.sh`, `--election-type unclean`, `--delete` d'un topic ou d'un groupe. En production : `--dry-run` d'abord, export des offsets avant toute réinitialisation, validation par un second regard.
- **Ajout de partitions** : il change la partition des clés et casse l'ordre par clé pendant la transition. À planifier avec les équipes applicatives.
- **Arrêt brutal** : `docker compose kill` sert à simuler une panne. En production, un arrêt propre évite une indisponibilité de plusieurs secondes et une vérification des fichiers au redémarrage, qui peut durer plusieurs minutes sur de gros volumes.
- **`scripts/reset.sh`** : propre au lab, il efface toutes les données.

---

## 13. Erreurs fréquentes

- **`command not found: kafka-topics.sh`** : la commande a été lancée sur l'hôte. Ouvrez d'abord `docker compose exec tools bash`.
- **`no configuration file provided: not found`** : `docker compose` a été lancé hors du dossier `lab/`. Faites `cd lab`.
- **`--bootstrap-server must be specified`** : ajoutez `--bootstrap-server $BS`.
- **`Connection to node -1 (localhost/127.0.0.1:9092) could not be established`** : dans `tools`, `localhost` désigne le conteneur lui-même. Utilisez `$BS`.
- **`$BS` vide sur l'hôte** : la variable n'existe que dans `tools`. Écrivez la liste des brokers en entier, ou passez par `docker compose exec tools bash -c '...'`.
- **`Assignments can only be reset if the group ... is inactive`** : arrêtez tous les consommateurs du groupe avant `--reset-offsets`.
- **`would not be an increase`** : le nombre de partitions d'un topic ne peut pas diminuer.
- **`Topic ... not present in metadata`** : le topic n'existe pas, et le lab ne crée pas les topics automatiquement.
- **Un nœud reste `health: starting`** : souvent un manque de mémoire Docker. Regardez `docker compose logs kafka-N --tail 50`, ou passez en mode `core`.
- **`grep` ne trouve pas une trace attendue** : le fichier a tourné. Cherchez dans `nom.log*` et sur les 3 nœuds.
