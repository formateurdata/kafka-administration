# TP2 : topics et messages en ligne de commande

| Repère | Valeur |
|---|---|
| Moment | Jour 1, 11:50-12:30, après la démo d'ouverture du TP (11:45-11:50) |
| Compétences | C4 Administrer les topics ; C5 Produire et consommer |
| Slides de rappel | S6 (topics et partitions), S7 (offsets et ordre), S8 (partitionnement avec et sans clé), S22 (outils de `bin/`) |
| Solution formateur | `corrige.md` (point 13) |
| Version notebook | `enonce.ipynb` dans JupyterLab (http://localhost:8888), mêmes étapes en Python ; démo d'ouverture : `demos-tp/demo-tp2.ipynb` |

Conventions : `# [hôte]` sur votre machine dans `lab/`, `# [tools]` dans `docker compose exec tools bash`. Les sections 7, 8 et 9 utilisent la même numérotation.

---

## 1. Objectifs

À la fin du TP, vous savez :

- constater que la création automatique des topics est désactivée, et pourquoi ;
- créer, décrire, modifier et supprimer un topic ;
- lire le placement des répliques : leader, répliques, ISR ;
- produire des messages avec clé depuis un fichier, et sans clé ;
- consommer en affichant clé, partition et offset ;
- vérifier que tous les messages d'une même clé arrivent dans la même partition ;
- mesurer l'effet d'un ajout de partitions sur la répartition des clés ;
- poser, lire et retirer une configuration de topic.

## 2. Durée

40 minutes, après 5 minutes de démonstration d'ouverture.

| Étape | Durée |
|---|---|
| 7.1 Produire vers un topic absent | 3 min |
| 7.2 Créer le topic `orders` | 4 min |
| 7.3 Décrire le topic | 3 min |
| 7.4 Produire avec clé | 4 min |
| 7.5 Lire les offsets et consommer | 5 min |
| 7.6 Défi : correspondance clé et partition | 5 min |
| 7.7 Ajouter des partitions | 6 min |
| 7.8 Configurer le topic `clickstream` | 5 min |
| 7.9 Produire sans clé | 3 min |
| 7.10 Kafka UI et suppression d'un topic | 2 min |

## 3. Prérequis

- TP1 terminé : lab démarré et `scripts/validate.sh` au vert.
- Un shell `tools` ouvert : `docker compose exec tools bash`.
- Jeu de données `/opt/lab/data/orders.txt` : 30 commandes au format `client|commande JSON`, clients `C001` à `C007`.

## 4. Architecture utilisée

```
  tools ($BS) ──> kafka-1, kafka-2, kafka-3 (listener INTERNAL, port 19092)

  Topic orders, 3 partitions, RF 3 : chaque partition a 1 leader et 2 followers,
  répartis sur les 3 brokers.

            kafka-1          kafka-2          kafka-3
  orders-0  follower         LEADER           follower
  orders-1  follower         follower         LEADER
  orders-2  LEADER           follower         follower
```

Le placement exact des leaders varie d'un poste à l'autre.

## 5. Contexte métier

Le service Commandes de NovaShop va publier chaque commande dans un topic `orders`. Exigence métier : les commandes d'un même client doivent être traitées dans l'ordre. L'équipe Data veut aussi un topic `clickstream` pour la navigation, conservé 24 heures.

Vous préparez ces topics et vérifiez que la clé de partitionnement garantit bien l'ordre par client.

## 6. Énoncé

1. Constatez que produire vers un topic qui n'existe pas échoue.
2. Créez `orders` (3 partitions, facteur de réplication 3) et alimentez-le avec `orders.txt`, en utilisant l'identifiant client comme clé.
3. Prouvez que toutes les commandes d'un même client sont dans la même partition.
4. Passez `orders` à 6 partitions, alimentez-le à nouveau et mesurez ce que l'opération change pour l'ordre par client.
5. Créez `clickstream` (6 partitions) avec une rétention de 24 heures.

## 7. Étapes

**7.1 Produire vers un topic absent.** Envoyez un message vers `orders`, qui n'existe pas encore. Observez les avertissements puis l'erreur.

**7.2 Créer le topic `orders`.** Créez-le avec 3 partitions et un facteur de réplication de 3. Essayez ensuite de créer un topic avec un facteur de réplication de 4.

**7.3 Décrire le topic.** Pour chaque partition, relevez le leader, les répliques et l'ISR.

**7.4 Produire avec clé.** Envoyez le contenu de `orders.txt` en déclarant que la clé est séparée de la valeur par `|`.

**7.5 Lire les offsets et consommer.** Affichez le dernier offset de chaque partition. Consommez les 5 premiers messages de la partition 0 en affichant partition, offset et clé.

**7.6 Défi : correspondance clé et partition.** Écrivez une commande qui affiche, pour chaque client, la ou les partitions où se trouvent ses commandes. Indice : le consommateur peut afficher la partition et la clé ; `awk` et `sort -u` font le reste.

**7.7 Ajouter des partitions.** Passez `orders` à 6 partitions, renvoyez `orders.txt`, puis relancez la commande du défi. Essayez ensuite de revenir à 3 partitions.

**7.8 Configurer le topic `clickstream`.** Créez-le avec 6 partitions. Posez une rétention de 24 heures et une limite de 100 Mo par partition. Affichez sa configuration, listez les topics qui ont des configurations particulières, puis retirez la limite de taille.

**7.9 Produire sans clé.** Envoyez 1000 messages sans clé dans `clickstream` et regardez leur répartition par partition.

**7.10 Kafka UI et suppression.** Retrouvez les messages de `orders` dans Kafka UI. Créez puis supprimez un topic `tmp-test`.

## 8. Commandes directement exécutables

### 8.1 Produire vers un topic absent

```bash
# [tools]
echo "test" | kafka-console-producer.sh --bootstrap-server $BS --topic orders \
  --command-property max.block.ms=5000
```

### 8.2 Créer le topic `orders`

```bash
# [tools]
kafka-topics.sh --bootstrap-server $BS --create --topic orders --partitions 3 --replication-factor 3
kafka-topics.sh --bootstrap-server $BS --create --topic orders-rf4 --partitions 1 --replication-factor 4
```

### 8.3 Décrire le topic

```bash
# [tools]
kafka-topics.sh --bootstrap-server $BS --describe --topic orders
```

### 8.4 Produire avec clé

```bash
# [tools]
head -3 /opt/lab/data/orders.txt
kafka-console-producer.sh --bootstrap-server $BS --topic orders \
  --reader-property parse.key=true --reader-property 'key.separator=|' \
  < /opt/lab/data/orders.txt
```

### 8.5 Lire les offsets et consommer

```bash
# [tools]
kafka-get-offsets.sh --bootstrap-server $BS --topic orders
kafka-console-consumer.sh --bootstrap-server $BS --topic orders --partition 0 --offset 0 --max-messages 5 \
  --formatter-property print.key=true --formatter-property print.partition=true \
  --formatter-property print.offset=true
```

### 8.6 Défi

À vous d'écrire la commande. Elle doit afficher une ligne par couple (client, partition), par exemple `C002 Partition:0`.

### 8.7 Ajouter des partitions

```bash
# [tools]
kafka-topics.sh --bootstrap-server $BS --alter --topic orders --partitions 6
kafka-console-producer.sh --bootstrap-server $BS --topic orders \
  --reader-property parse.key=true --reader-property 'key.separator=|' \
  < /opt/lab/data/orders.txt
kafka-get-offsets.sh --bootstrap-server $BS --topic orders
# Relancez ici votre commande du défi (60 messages au lieu de 30).
kafka-topics.sh --bootstrap-server $BS --alter --topic orders --partitions 3
```

### 8.8 Configurer le topic `clickstream`

```bash
# [tools]
kafka-topics.sh --bootstrap-server $BS --create --topic clickstream --partitions 6 --replication-factor 3
kafka-configs.sh --bootstrap-server $BS --alter --entity-type topics --entity-name clickstream \
  --add-config retention.ms=86400000,retention.bytes=104857600
kafka-configs.sh --bootstrap-server $BS --describe --entity-type topics --entity-name clickstream
kafka-topics.sh --bootstrap-server $BS --describe --topics-with-overrides
kafka-configs.sh --bootstrap-server $BS --alter --entity-type topics --entity-name clickstream \
  --delete-config retention.bytes
```

### 8.9 Produire sans clé

```bash
# [tools]
seq 1 1000 | sed 's/^/click-/' | kafka-console-producer.sh --bootstrap-server $BS --topic clickstream
kafka-get-offsets.sh --bootstrap-server $BS --topic clickstream
```

### 8.10 Kafka UI et suppression

Kafka UI : http://localhost:8080, menu « Topics », topic `orders`, onglet des messages.

```bash
# [tools]
kafka-topics.sh --bootstrap-server $BS --create --topic tmp-test --partitions 1 --replication-factor 3
kafka-topics.sh --bootstrap-server $BS --delete --topic tmp-test
kafka-topics.sh --bootstrap-server $BS --list
```

## 9. Résultats attendus

Les leaders et les identifiants de topic varient. La répartition des clés est identique sur tous les postes : elle ne dépend que de la clé et du nombre de partitions.

### 9.1 Topic absent

Des avertissements `{orders=UNKNOWN_TOPIC_OR_PARTITION}`, puis au bout de 5 s :

```
org.apache.kafka.common.errors.TimeoutException: Topic orders not present in metadata after 5000 ms.
```

### 9.2 Création

`Created topic orders.` Le facteur de réplication de 4 est refusé :

```
Unable to replicate the partition 4 time(s): The target replication factor of 4 cannot be reached because only 3 broker(s) are registered or some brokers have all their log directories cordoned.
```

### 9.3 Description

3 lignes de partition. Chaque partition a 3 répliques, toutes dans l'ISR, et des leaders répartis sur les 3 brokers. La ligne de titre affiche aussi des configurations héritées du cluster : `min.insync.replicas=1`, `segment.bytes=1073741824`, `unclean.leader.election.enable=false`.

### 9.4 et 9.5 Production et offsets

```
orders:0:6
orders:1:8
orders:2:16
```

Les 5 premiers messages de la partition 0 sont tous du client `C002`.

### 9.6 Défi

Chaque client apparaît dans une seule partition : `C002` en 0, `C006` et `C007` en 1, `C001`, `C003`, `C004` et `C005` en 2.

### 9.7 Ajout de partitions

Après le second envoi : `orders:0:12`, `orders:1:16`, `orders:2:21`, `orders:3:0`, `orders:4:0`, `orders:5:11`. Les clients `C001`, `C003` et `C004` apparaissent maintenant dans deux partitions, 2 et 5. Le retour à 3 partitions est refusé : `3 would not be an increase`.

### 9.8 Configuration de `clickstream`

`describe` affiche `retention.ms=86400000` et `retention.bytes=104857600` avec la source `DYNAMIC_TOPIC_CONFIG`. Après `--delete-config`, seule `retention.ms` reste.

### 9.9 Sans clé

Les 1000 messages se retrouvent tous, ou presque tous, dans une seule partition.

### 9.10 Suppression

`tmp-test` n'apparaît plus dans la liste.

## 10. Vérification

```bash
# [tools]
kafka-topics.sh --bootstrap-server $BS --describe --topic orders | head -1
kafka-get-offsets.sh --bootstrap-server $BS --topic orders | awk -F: '{s += $3} END {print "messages dans orders :", s}'
kafka-configs.sh --bootstrap-server $BS --describe --entity-type topics --entity-name clickstream
```

| Contrôle | Attendu |
|---|---|
| `orders` | 6 partitions, RF 3 |
| Messages dans `orders` | 60 |
| `clickstream` | 6 partitions, `retention.ms=86400000` seule configuration dynamique |

Ces topics sont réutilisés au TP3 : ne les supprimez pas.

## 11. Questions de réflexion

1. Pourquoi désactiver la création automatique des topics en production ?
2. Le facteur de réplication peut-il dépasser le nombre de brokers ? Et le nombre de partitions ?
3. Pourquoi Kafka refuse-t-il de réduire le nombre de partitions ?
4. Après l'ajout de partitions, l'ordre des commandes du client `C001` est-il encore garanti ? Que conseilleriez-vous à l'équipe Commandes ?
5. Pourquoi 1000 messages sans clé se retrouvent-ils dans une seule partition, alors qu'on parle souvent de répartition « round-robin » ?
6. Comment choisir le nombre de partitions d'un nouveau topic ?

## 12. Erreurs fréquentes

| Symptôme | Cause | Correction |
|---|---|---|
| Clé `null` à la lecture | `parse.key=true` oublié à la production | Renvoyer avec `--reader-property parse.key=true` |
| Clé égale à toute la ligne, ou valeur coupée | Séparateur par défaut (tabulation) au lieu de `\|` | `--reader-property 'key.separator=\|'`, avec les guillemets simples |
| Aucun message lu, la commande attend | Consommateur sans `--from-beginning` : il démarre à la fin du topic | Ajouter `--from-beginning`, ou `--partition N --offset 0` |
| `Processed a total of 0 messages` puis arrêt | `--timeout-ms` trop court | Augmenter ou retirer `--timeout-ms` |
| `Topic 'orders' already exists` | Création relancée | Normal ; utiliser `--if-not-exists` dans les scripts |
| `--delete-config` sans effet visible | Nom de configuration mal orthographié | Vérifier avec `--describe` ; en 4.3, supprimer une clé absente ne produit pas d'erreur |

## 13. Solution formateur

Voir `corrige.md`.

## 14. Variante avancée

1. Créez un topic `returns` dont vous imposez le placement : 3 partitions, 2 répliques, la partition 0 sur les brokers 1 et 2, la 1 sur 2 et 3, la 2 sur 3 et 1. Vérifiez le placement, puis supprimez le topic. Option : `--replica-assignment`.
2. Affichez toutes les configurations effectives de `clickstream` avec `--all`. Pour `retention.ms`, `retention.bytes`, `segment.bytes`, `min.insync.replicas` et `cleanup.policy`, indiquez d'où vient la valeur.
3. Lisez exactement 3 messages de la partition 2 de `orders` à partir de l'offset 10.
