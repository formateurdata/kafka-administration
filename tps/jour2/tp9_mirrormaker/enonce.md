# TP9 : répliquer un cluster vers le cloud avec MirrorMaker 2

| Repère | Valeur |
|---|---|
| Moment | Jour 2, 14:15-15:10, après la démo d'ouverture du TP (14:10-14:15) |
| Compétences | C18 Réplication entre clusters avec MirrorMaker 2, supervision, bascule d'un consumer group ; rappel de C9 (SASL_SSL, SCRAM) et C10 (ACL) |
| Slides de rappel | S75 (architecture de MirrorMaker 2), S76 (configurer un flux), S77 (offsets et bascule), S78 (superviser) |
| Solution formateur | `corrige.md` (point 13) |
| Version notebook | `enonce.ipynb` dans JupyterLab (http://localhost:8888), mêmes étapes en Python ; démo d'ouverture : `demos-tp/demo-tp9.ipynb` |

Conventions : `# [hôte]` sur votre machine dans `lab/`, `# [tools]` dans `docker compose exec tools bash`. Les sections 7, 8 et 9 utilisent la même numérotation.

Gardez **deux terminaux** : A sur l'hôte, B dans `tools`. Dans `tools`, `$BS` vise le cluster onprem et `$BS_CLOUD` le cluster cloud.

---

## 1. Objectifs

À la fin du TP, vous savez :

- démarrer un second cluster et le distinguer du premier ;
- lire une configuration de MirrorMaker 2 : clusters, flux, topics, topics internes ;
- diagnostiquer un MirrorMaker 2 qui ne réplique pas, alors que son conteneur tourne ;
- donner à MirrorMaker 2 les seuls droits dont il a besoin sur le cluster cible ;
- basculer un consumer group d'un cluster à l'autre, et mesurer les messages relus ;
- mesurer le retard de réplication pendant une coupure, puis le rattrapage.

## 2. Durée

55 minutes, après 5 minutes de démonstration d'ouverture.

| Étape | Durée |
|---|---|
| 7.1 Démarrer le cluster cloud | 5 min |
| 7.2 Lire la configuration de MirrorMaker 2 | 5 min |
| 7.3 Lancer MirrorMaker 2 sans droits | 7 min |
| 7.4 Donner les droits, constater la réplication | 10 min |
| 7.5 Basculer un consumer group | 14 min |
| 7.6 Coupure et rattrapage | 7 min |
| 7.7 Défi : ajouter un topic, propager une configuration | 7 min |

## 3. Prérequis

- Lab en mode `extended`, sain : `scripts/validate.sh`.
- Topics `orders`, `payments` et `refunds` présents, issus des TP2 et TP4.
- Certificat du cluster cloud présent : `security/certs/kafka-cloud.keystore.p12`. Sinon : `bash scripts/prepare.sh --no-pull`.
- 1 Go de mémoire disponible pour Docker, en plus du mode `extended`.
- Notions : slides S74 à S78 ; SASL_SSL, SCRAM et ACL du TP5.

## 4. Architecture utilisée

```
  Cluster onprem (source)                         Cluster cloud (cible, joue AWS MSK)
  kafka-1, kafka-2, kafka-3                       kafka-cloud, noeud unique
  orders, payments, refunds                       onprem.orders, onprem.payments
            |                                              ^
            |  lit (INTERNAL :19092)                       |  ecrit (SASL_SSL :9096, compte mm2)
            +-------------->  mirrormaker  ----------------+
                              MirrorMaker 2, mode dedie
                              config/mirrormaker/mm2.properties
```

| Élément | Valeur |
|---|---|
| Profil Compose | `mirror` : `kafka-cloud` et `mirrormaker` |
| Alias des clusters | `onprem` et `cloud` : `orders` devient `onprem.orders` sur le cluster cloud |
| Cluster cloud, outils | `$BS_CLOUD` = `kafka-cloud:19092` (INTERNAL, sans authentification) |
| Cluster cloud, MirrorMaker 2 | `kafka-cloud:9096`, SASL_SSL et SCRAM-SHA-512, compte `mm2` (le port SASL/SCRAM de MSK) |

| Lab | Production |
|---|---|
| Cluster cible d'un seul nœud, facteur de réplication 1 | 3 brokers ou plus sur 3 zones, RF 3 (MSK : 2 ou 3 zones) |
| Intervalles de synchronisation de 5 à 10 s | Défauts : 60 s pour les offsets, 10 min pour les configurations et la liste des topics |
| Mot de passe de `mm2` en clair dans `mm2.properties` | Secret externalisé : fournisseur de configuration, gestionnaire de secrets (AWS Secrets Manager pour MSK) |
| Authentification SCRAM | MSK : SCRAM ou IAM (`aws-msk-iam-auth`) ; IAM n'est pas testé dans ce lab |
| MirrorMaker 2 lit la source sans authentification | Un compte sur la source aussi, avec ses ACL de lecture |

## 5. Contexte métier

NovaShop ouvre un cluster géré dans le cloud. Les applications d'analyse y seront déplacées en premier : elles doivent y retrouver les commandes et les paiements, en continu, sans que les applications du site soient touchées. Le jour de la bascule, l'équipe Expédition passera son application du cluster du site au cluster cloud : elle veut savoir combien de messages elle relira.

## 6. Énoncé

1. Démarrez le cluster cloud et prouvez qu'il est distinct du cluster onprem.
2. Lisez la configuration de MirrorMaker 2 : topics, sens, emplacement des topics internes.
3. Démarrez MirrorMaker 2 et prouvez qu'il ne réplique pas. Trouvez pourquoi.
4. Donnez au compte `mm2` les droits nécessaires sur le cluster cloud, et prouvez que `orders` et `payments` y sont recopiés, sans rien écrire sur le cluster onprem.
5. Basculez le groupe `expedition` du cluster onprem au cluster cloud, et mesurez les messages relus.
6. Coupez MirrorMaker 2 pendant une rafale de commandes : mesurez le retard, puis le rattrapage.
7. Défi : ajoutez `refunds` à la réplication, et vérifiez qu'une configuration de topic est propagée.

## 7. Étapes

**7.1 Démarrer le cluster cloud.** Lisez les paramètres clés de `config/kafka/kafka-cloud/server.properties` : rôles, listeners, `super.users`, `allow.everyone.if.no.acl.found`, facteur de réplication par défaut. Démarrez `kafka-cloud` avec le profil `mirror` et lisez ses traces. Comparez les identifiants des deux clusters. Listez les topics du cluster cloud, et le compte SCRAM qui y existe déjà.

**7.2 Lire la configuration de MirrorMaker 2.** Affichez `config/mirrormaker/mm2.properties` sans les commentaires. Répondez par écrit : quels topics sont recopiés ? Dans quel sens ? Sur quel cluster vont les topics internes de MirrorMaker 2 ? À quoi sert la ligne `cloud->onprem.emit.heartbeats.enabled = false` ?

**7.3 Lancer MirrorMaker 2 sans droits.** Démarrez `mirrormaker`. Après 30 s, regardez l'état du conteneur, puis ses traces. Listez les topics du cluster cloud. Concluez : MirrorMaker 2 réplique-t-il ? Pourquoi ?

**7.4 Donner les droits, constater la réplication.** Créez sur le cluster cloud les quatre ACL du compte `mm2` (tableau ci-dessous), puis redémarrez `mirrormaker`. Listez les topics du cluster cloud. Comparez le nombre de messages de `orders` et `onprem.orders`, de `payments` et `onprem.payments`. Vérifiez qu'aucun topic `mm2-*` ni `heartbeats` n'a été créé sur le cluster onprem. Comparez la description de `payments` et celle d'`onprem.payments`.

| Ressource | Motif | Pour |
|---|---|---|
| Topics commençant par `onprem.` | `prefixed` | Les topics recopiés, `onprem.checkpoints.internal` |
| Topics commençant par `mm2-` | `prefixed` | Les topics internes de Connect et `mm2-offset-syncs.onprem.internal` |
| Topic `heartbeats` | `literal` | Les heartbeats émis par le flux |
| Tous les groupes (`'*'`) | `literal` | Le groupe de Connect et l'écriture des offsets traduits des groupes |

**7.5 Basculer un consumer group.** Lisez 40 messages d'`orders` sur le cluster onprem avec le groupe `expedition`. Décrivez le groupe sur le cluster onprem : notez, par partition, l'offset commité et le lag. Attendez 20 s, puis décrivez le groupe `expedition` sur le cluster cloud. Comparez les offsets partition par partition : seules les partitions où le groupe a lu apparaissent. **Écrivez votre prévision** : combien de messages l'application lira-t-elle sur le cloud ? Puis lisez `onprem.orders` sur le cluster cloud avec le groupe `expedition` et comptez les messages. Combien de doublons ?

**7.6 Coupure et rattrapage.** Arrêtez `mirrormaker`. Envoyez 3000 messages de 200 octets dans `orders`. Mesurez l'écart entre `orders` et `onprem.orders`. Redémarrez `mirrormaker` et mesurez le temps de rattrapage.

**7.7 Défi : ajouter un topic, propager une configuration.** Ajoutez `refunds` à la ligne `onprem->cloud.topics` de `mm2.properties`, redémarrez `mirrormaker`, et vérifiez qu'`onprem.refunds` a autant de messages que `refunds`. Faut-il une nouvelle ACL ? Passez ensuite `retention.ms` d'`orders` à 259200000 (3 jours) sur le cluster onprem, et vérifiez que la valeur arrive sur `onprem.orders`.

**Fin du TP.** Arrêtez le profil `mirror` : le TP8 ne l'utilise pas.

## 8. Commandes directement exécutables

### 8.1 Cluster cloud

```bash
# [hôte] terminal A
grep -E '^(process.roles|node.id|listeners|super.users|allow.everyone.if.no.acl.found|default.replication.factor)=' \
  config/kafka/kafka-cloud/server.properties
docker compose --profile mirror up -d kafka-cloud
docker compose logs kafka-cloud | grep -E 'start-cloud|Formatting|Kafka Server started'
```

```bash
# [tools] terminal B
kafka-cluster.sh cluster-id --bootstrap-server $BS
kafka-cluster.sh cluster-id --bootstrap-server $BS_CLOUD
kafka-topics.sh --bootstrap-server $BS_CLOUD --list
kafka-configs.sh --bootstrap-server $BS_CLOUD --describe --entity-type users
```

### 8.2 Configuration de MirrorMaker 2

```bash
# [hôte]
grep -Ev '^(#|$)' config/mirrormaker/mm2.properties
```

### 8.3 MirrorMaker 2 sans droits

```bash
# [hôte]
docker compose --profile mirror up -d mirrormaker
sleep 30
docker compose ps mirrormaker
docker compose logs mirrormaker | grep -E 'AuthorizationException|herder work thread' | cut -c1-220 | head -4
```

```bash
# [tools]
kafka-topics.sh --bootstrap-server $BS_CLOUD --list
```

### 8.4 Droits et réplication

```bash
# [tools]
kafka-acls.sh --bootstrap-server $BS_CLOUD --add --allow-principal User:mm2 --operation All \
  --topic onprem. --resource-pattern-type prefixed
kafka-acls.sh --bootstrap-server $BS_CLOUD --add --allow-principal User:mm2 --operation All \
  --topic mm2- --resource-pattern-type prefixed
kafka-acls.sh --bootstrap-server $BS_CLOUD --add --allow-principal User:mm2 --operation All --topic heartbeats
kafka-acls.sh --bootstrap-server $BS_CLOUD --add --allow-principal User:mm2 --operation All --group '*'
```

```bash
# [hôte]
docker compose --profile mirror restart mirrormaker
```

```bash
# [tools] après une trentaine de secondes
kafka-topics.sh --bootstrap-server $BS_CLOUD --list
for t in orders payments; do
  echo "$t : $(kafka-get-offsets.sh --bootstrap-server $BS --topic $t | awk -F: '{s += $3} END {print s}')" \
       "onprem.$t : $(kafka-get-offsets.sh --bootstrap-server $BS_CLOUD --topic onprem.$t | awk -F: '{s += $3} END {print s}')"
done
kafka-topics.sh --bootstrap-server $BS --list | grep -E '^(mm2-|heartbeats)' || echo "rien sur la source"
kafka-topics.sh --bootstrap-server $BS --describe --topic payments | head -1
kafka-topics.sh --bootstrap-server $BS_CLOUD --describe --topic onprem.payments | head -1
```

### 8.5 Bascule du groupe `expedition`

```bash
# [tools]
kafka-console-consumer.sh --bootstrap-server $BS --topic orders --group expedition \
  --from-beginning --max-messages 40 > /dev/null
kafka-consumer-groups.sh --bootstrap-server $BS --describe --group expedition
sleep 20
kafka-consumer-groups.sh --bootstrap-server $BS_CLOUD --describe --group expedition
# votre prevision, puis :
kafka-console-consumer.sh --bootstrap-server $BS_CLOUD --topic onprem.orders --group expedition \
  --timeout-ms 10000 | wc -l
```

### 8.6 Coupure et rattrapage

```bash
# [hôte]
docker compose --profile mirror stop mirrormaker
```

```bash
# [tools]
kafka-producer-perf-test.sh --bootstrap-server $BS --topic orders --num-records 3000 --record-size 200 \
  --throughput -1 | tail -1
fin() { kafka-get-offsets.sh --bootstrap-server $1 --topic $2 | awk -F: '{s += $3} END {print s}'; }
echo "retard : $(( $(fin $BS orders) - $(fin $BS_CLOUD onprem.orders) ))"
```

```bash
# [hôte]
docker compose --profile mirror start mirrormaker
```

```bash
# [tools] à relancer jusqu'à un retard nul ; noter l'heure
date +%T; echo "retard : $(( $(fin $BS orders) - $(fin $BS_CLOUD onprem.orders) ))"
```

### 8.7 Défi

```bash
# [hôte]
sed -i.bak 's/^onprem->cloud.topics = orders, payments$/onprem->cloud.topics = orders, payments, refunds/' \
  config/mirrormaker/mm2.properties && rm config/mirrormaker/mm2.properties.bak
grep '^onprem->cloud.topics' config/mirrormaker/mm2.properties
docker compose --profile mirror restart mirrormaker
```

```bash
# [tools]
echo "refunds : $(fin $BS refunds) - onprem.refunds : $(fin $BS_CLOUD onprem.refunds)"
kafka-configs.sh --bootstrap-server $BS --alter --entity-type topics --entity-name orders \
  --add-config retention.ms=259200000
sleep 20
kafka-configs.sh --bootstrap-server $BS_CLOUD --describe --entity-type topics --entity-name onprem.orders
```

### 8.8 Fin du TP

```bash
# [hôte]
docker compose --profile mirror stop mirrormaker kafka-cloud
```

## 9. Résultats attendus

### 9.1 Cluster cloud

- `process.roles=broker,controller`, `node.id=1`, `listeners=CONTROLLER://:9093,INTERNAL://:19092,SECURE://:9096`, `super.users=User:admin;User:ANONYMOUS`, `allow.everyone.if.no.acl.found=false`, `default.replication.factor=1`.
- Traces : `[start-cloud] Formatage du stockage (cluster tF2HiBQuSD6MqpTGGGBVfA)`, puis `Kafka Server started`.
- `Cluster ID: W5FmqobtRp-BMcXlZhVKBw` pour onprem, `Cluster ID: tF2HiBQuSD6MqpTGGGBVfA` pour cloud.
- Aucun topic sur le cluster cloud.
- `SCRAM credential configs for user-principal 'mm2' are SCRAM-SHA-512=iterations=4096`.

### 9.2 Configuration

- Flux `onprem->cloud` activé pour `orders, payments`, tous les groupes (`.*`) ; sens `cloud->onprem` désactivé, sans heartbeats.
- Cible jointe en `SASL_SSL` et `SCRAM-SHA-512` sur `kafka-cloud:9096`.
- `offset-syncs.topic.location = target` : les topics internes vont sur le cluster cloud.

### 9.3 Sans droits

- `docker compose ps` : `mirrormaker` est `Up`.
- Traces : `TopicAuthorizationException: Not authorized to access topics: [mm2-offsets.onprem.internal]`, puis `Uncaught exception in herder work thread, exiting`.
- Aucun topic sur le cluster cloud : MirrorMaker 2 ne réplique pas, alors que son conteneur tourne.

### 9.4 Réplication

- Chaque `kafka-acls.sh --add` affiche l'ACL créée.
- Sur le cluster cloud : `__consumer_offsets`, `heartbeats`, `mm2-configs.onprem.internal`, `mm2-offset-syncs.onprem.internal`, `mm2-offsets.onprem.internal`, `mm2-status.onprem.internal`, `onprem.checkpoints.internal`, `onprem.orders`, `onprem.payments`.
- Même nombre de messages des deux côtés pour `orders` et `payments`.
- `rien sur la source`.
- `onprem.payments` a le même nombre de partitions que `payments`, avec `ReplicationFactor: 1`.

### 9.5 Bascule

- Le groupe `expedition` a des offsets sur `orders` côté onprem, et sur `onprem.orders` côté cloud.
- Sur au moins une partition, l'offset du cloud est inférieur à celui de la source.
- La lecture sur le cloud rend **plus** de messages que le lag total affiché côté onprem : la différence, ce sont les doublons. Aucun message n'est perdu.

### 9.6 Coupure

- Pendant la coupure : `retard : 3000`.
- Après le redémarrage, le retard revient à 0 en une dizaine de secondes, démarrage de la JVM compris (6 à 13 s sur le lab).

### 9.7 Défi

- `onprem->cloud.topics = orders, payments, refunds`.
- `onprem.refunds` apparaît sur le cluster cloud, avec autant de messages que `refunds`. Aucune nouvelle ACL : le préfixe `onprem.` la couvre.
- `retention.ms=259200000` dans les configurations dynamiques d'`onprem.orders`.

## 10. Vérification

| Contrôle | Commande | Attendu |
|---|---|---|
| Réplication active | `bash scripts/validate.sh` (sur l'hôte, profil `mirror` démarré) | `MirrorMaker 2 actif (heartbeats du cluster cloud : ...)` |
| Même nombre de messages | `fin $BS orders` et `fin $BS_CLOUD onprem.orders` | Égaux |
| Source intacte | `kafka-topics.sh --bootstrap-server $BS --list \| grep -E '^(mm2-\|heartbeats)'` | Rien |
| Groupe traduit | `kafka-consumer-groups.sh --bootstrap-server $BS_CLOUD --describe --group expedition` | Offsets sur `onprem.orders` |
| ACL | `kafka-acls.sh --bootstrap-server $BS_CLOUD --list --principal User:mm2` | 4 ressources |

## 11. Questions de réflexion

1. Le conteneur `mirrormaker` était `Up` en 7.3. Quelle supervision aurait détecté le problème ?
2. Pourquoi les topics recopiés s'appellent-ils `onprem.orders` et non `orders` ? Dans quel cas voudrait-on garder le même nom ?
3. Pourquoi l'application Expédition relit-elle des messages après la bascule ? Comment s'en protéger ?
4. MirrorMaker 2 est resté arrêté 2 minutes en 7.6. Qu'est-ce qui aurait été perdu s'il était resté arrêté 8 jours ?
5. Pourquoi placer les topics internes de MirrorMaker 2 sur le cluster cible ?
6. Le compte `mm2` a `All` sur tous les groupes du cluster cloud. Comment réduire ces droits ?
7. Chez vous, MirrorMaker 2 relie des clusters on-premise à AWS MSK. Qu'est-ce qui change par rapport au lab ?
8. Un script défectueux écrit des messages erronés dans `orders` sur le cluster onprem. MirrorMaker 2 les recopie-t-il ? Une réplique sur le cluster cloud remplace-t-elle une sauvegarde ?
9. Pourquoi relier deux clusters par MirrorMaker 2 plutôt qu'étendre un seul cluster sur les deux sites ?

## 12. Erreurs fréquentes

| Symptôme | Cause | Correction |
|---|---|---|
| `kafka-cloud` s'arrête au démarrage, `NoSuchFileException: .../kafka-cloud.keystore.p12` | Lab préparé avant l'ajout du cluster cloud : certificat absent | `bash scripts/prepare.sh --no-pull`, puis redémarrer `kafka-cloud` |
| Conteneur `mirrormaker` `Up`, aucun topic recopié | MirrorMaker 2 refusé (ACL absente) ou cluster cloud injoignable : le processus reste en vie | Lire `docker compose logs mirrormaker` ; ajouter l'ACL ; `restart mirrormaker` |
| `SaslAuthenticationException: Authentication failed during authentication due to invalid credentials` | Mot de passe de `mm2` modifié d'un côté seulement | Même valeur dans `mm2.properties` et `MM2_PASSWORD` ; le compte n'est créé qu'au premier formatage du volume |
| `--group *` sans apostrophes : ACL créée sur un nom de fichier | Le shell remplace `*` par les fichiers du dossier courant | Toujours `--group '*'` |
| Les offsets du groupe ne sont pas traduits sur le cloud | Un consommateur du groupe est actif sur le cloud, ou l'intervalle de synchronisation n'est pas écoulé | Arrêter le consommateur côté cloud ; attendre 10 s |
| Un topic ajouté à `topics` n'est pas recopié | MirrorMaker 2 non redémarré, ou topic absent de la source | `restart mirrormaker` ; vérifier le nom sur la source |
| `heartbeats` ou `mm2-*` apparaissent sur le cluster onprem | Ligne `cloud->onprem.emit.heartbeats.enabled = false` retirée | La remettre ; supprimer ces topics de la source |

## 13. Solution formateur

Voir `corrige.md`.

## 14. Variante avancée

1. **Moins de doublons.** Envoyez 1200 messages à clé dans `orders`, placez un nouveau groupe 100 messages avant la fin de chaque partition (`kafka-consumer-groups.sh --reset-offsets --shift-by -100` après un `--to-latest`), puis basculez-le comme en 7.5 dans deux cas : groupe **à jour** (il lit tout sur onprem avant la bascule) et groupe **en retard** (il ne lit que 40 messages). Ajoutez ensuite `onprem->cloud.offset.lag.max = 10` à `mm2.properties`, redémarrez MirrorMaker 2 et refaites les deux bascules avec de nouveaux groupes. Comparez les doublons. Quel est le coût de ce réglage ?
2. **Lire les checkpoints.** Lisez le topic `onprem.checkpoints.internal` du cluster cloud avec le formateur `org.apache.kafka.connect.mirror.formatters.CheckpointFormatter` de `kafka-console-consumer.sh`. Retrouvez-y le groupe `expedition` : que signifient `upstreamOffset` et `downstreamOffset` ?
