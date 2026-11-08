# TP3 : consumer groups, offsets, rétention et compaction

| Repère | Valeur |
|---|---|
| Moment | Jour 1, 14:05-15:00, après la démo d'ouverture du TP (14:00-14:05) |
| Compétences | C5 Produire, consommer, gérer les groupes et les offsets ; C6 Rétention et compaction |
| Slides de rappel | S11 (consumer groups), S12 (rééquilibrage et KIP-848), S29 (commits et `auto.offset.reset`), S31 (rétention et compaction) |
| Solution formateur | `corrige.md` (point 13) |
| Version notebook | `enonce.ipynb` dans JupyterLab (http://localhost:8888), mêmes étapes en Python ; démo d'ouverture : `demos-tp/demo-tp3.ipynb` |

Conventions : `# [hôte]` sur votre machine dans `lab/`, `# [tools]` dans `docker compose exec tools bash`. Les sections 7, 8 et 9 utilisent la même numérotation.

Ce TP utilise **trois terminaux** ouverts sur `tools`, appelés A, B et C. Ouvrez chacun avec `docker compose exec tools bash`.

---

## 1. Objectifs

À la fin du TP, vous savez :

- consommer avec un consumer group et lire son état, ses membres et son lag ;
- observer la répartition des partitions entre consommateurs et le rééquilibrage ;
- reconnaître un groupe utilisant le nouveau protocole (KIP-848) ;
- repositionner les offsets d'un groupe en sécurité : simulation, application, export ;
- faire supprimer des données par la rétention en temps ;
- configurer un topic compacté et supprimer une clé avec un tombstone.

## 2. Durée

55 minutes, après 5 minutes de démonstration d'ouverture.

| Étape | Durée |
|---|---|
| 7.1 Consommer avec un groupe | 5 min |
| 7.2 Créer et résorber du lag | 5 min |
| 7.3 Deux consommateurs dans un groupe | 7 min |
| 7.4 Départ d'un consommateur | 4 min |
| 7.5 Le nouveau protocole de groupe | 5 min |
| 7.6 Réinitialiser les offsets | 7 min |
| 7.7 Défi : retraiter la dernière heure | 4 min |
| 7.8 Rétention en temps | 7 min |
| 7.9 Compaction | 6 min |
| 7.10 Tombstone | 5 min |

## 3. Prérequis

- TP2 terminé : topic `orders` avec 6 partitions et 60 messages.
- Si ce n'est pas le cas, demandez au formateur le bloc de rattrapage du TP2.
- Jeu de données `/opt/lab/data/product-prices.txt` : 20 prix pour 4 produits `P-100` à `P-400`.
- Notions : slides S11, S29, S31.

## 4. Architecture utilisée

```
                       orders (6 partitions)
          p0    p1    p2    p3    p4    p5
           \     |     /     \     |     /
   groupe analytics : consommateur A (p0, p1, p2) + consommateur B (p3, p4, p5)
   groupe billing   : un consommateur, lancé puis arrêté (offsets conservés)

   Offsets validés : topic interne __consumer_offsets (50 partitions, compacté)
   Coordinateur du groupe : le leader de la partition de __consumer_offsets
   qui correspond au nom du groupe.
```

| Topic | Partitions | Configuration | Usage |
|---|---|---|---|
| `orders` | 6 | défaut | Groupes et offsets |
| `audit-short` | 1 | `retention.ms=60000`, `segment.ms=10000` | Rétention en temps |
| `product-prices` | 1 | `cleanup.policy=compact`, segments de 10 s | Compaction |

## 5. Contexte métier

Chez NovaShop, deux services lisent les commandes :

- **Facturation** (groupe `billing`) doit traiter chaque commande une fois, et pouvoir rejouer une période après un incident ;
- **Analytique** (groupe `analytics`) doit monter en charge en ajoutant des instances.

Le service Audit veut un journal court, et le service Catalogue un référentiel de prix qui ne garde que le dernier prix de chaque produit.

## 6. Énoncé

1. Faites consommer `orders` par la facturation, puis montrez le lag créé par de nouvelles commandes.
2. Faites travailler deux instances de l'analytique en parallèle, et observez ce qui se passe quand l'une s'arrête.
3. Repositionnez la facturation pour rejouer des commandes, sans jamais appliquer un changement que vous n'avez pas simulé.
4. Prouvez que la rétention supprime les vieux messages d'`audit-short` et que la compaction ne garde que le dernier prix de chaque produit.

## 7. Étapes

**7.1 Consommer avec un groupe.** Dans le terminal A, consommez tout `orders` avec le groupe `billing`. Puis décrivez le groupe.

**7.2 Créer et résorber du lag.** Envoyez 10 commandes du nouveau client `C008`. Décrivez le groupe `billing` : où est le lag ? Relancez la facturation et comptez les messages lus.

**7.3 Deux consommateurs dans un groupe.** Lancez un consommateur du groupe `analytics` dans le terminal A, puis un second dans le terminal B. Dans le terminal C, affichez les membres, leur affectation et l'état du groupe. Essayez ensuite de réinitialiser les offsets d'`analytics` pendant qu'il est actif.

**7.4 Départ d'un consommateur.** Arrêtez le consommateur du terminal B avec `Ctrl+C`. Dans C, observez l'état du groupe et la nouvelle affectation. Arrêtez ensuite le consommateur du terminal A.

**7.5 Le nouveau protocole de groupe.** Recommencez avec un groupe `analytics-v2` et l'option `group.protocol=consumer`. Comparez les colonnes affichées et la stratégie d'affectation. Listez les groupes avec leur type.

**7.6 Réinitialiser les offsets.** Sur le groupe `billing`, inactif :

- simulez un retour au début, puis appliquez-le, et mesurez le lag ;
- placez le groupe à la fin, puis reculez de 5 messages par partition ;
- exportez les offsets dans un fichier CSV.

**7.7 Défi : retraiter la dernière heure.** La facturation a mal traité toutes les commandes reçues depuis une heure. Repositionnez `billing` pour les retraiter. Simulez d'abord. Indice : `kafka-consumer-groups.sh --help`, cherchez une option qui accepte une durée au format ISO 8601.

**7.8 Rétention en temps.** Créez `audit-short` (rétention de 60 s, segments de 10 s), envoyez 100 messages, attendez 35 s puis envoyez-en un de plus. Surveillez le premier offset disponible : il passe à 100, puis à 101. Regardez les fichiers de segments sur `kafka-1`.

**7.9 Compaction.** Créez `product-prices`, compacté. Envoyez les 20 prix, attendez 12 s, puis envoyez le prix d'un nouveau produit `P-500`. Comptez les messages jusqu'à ce qu'il n'en reste qu'un par produit.

**7.10 Tombstone.** Supprimez le produit `P-500` du référentiel en envoyant un message de valeur nulle. Puis envoyez un message toutes les 15 s et observez dans quel ordre disparaissent l'ancienne valeur et le tombstone.

## 8. Commandes directement exécutables

### 8.1 Consommer avec un groupe

```bash
# [tools] terminal A
kafka-console-consumer.sh --bootstrap-server $BS --topic orders --group billing \
  --from-beginning --timeout-ms 10000 | wc -l
kafka-consumer-groups.sh --bootstrap-server $BS --describe --group billing
```

### 8.2 Créer et résorber du lag

```bash
# [tools] terminal A
for i in $(seq 1 10); do
  echo "C008|{\"order_id\":\"O-30$i\",\"customer\":\"C008\",\"amount\":10.0,\"city\":\"Lyon\"}"
done | kafka-console-producer.sh --bootstrap-server $BS --topic orders \
  --reader-property parse.key=true --reader-property 'key.separator=|'
kafka-consumer-groups.sh --bootstrap-server $BS --describe --group billing
kafka-console-consumer.sh --bootstrap-server $BS --topic orders --group billing --timeout-ms 10000 | wc -l
```

### 8.3 Deux consommateurs dans un groupe

```bash
# [tools] terminal A, puis la même commande dans le terminal B
kafka-console-consumer.sh --bootstrap-server $BS --topic orders --group analytics
```

```bash
# [tools] terminal C
kafka-consumer-groups.sh --bootstrap-server $BS --describe --group analytics --members --verbose
kafka-consumer-groups.sh --bootstrap-server $BS --describe --group analytics --state
kafka-consumer-groups.sh --bootstrap-server $BS --group analytics --topic orders \
  --reset-offsets --to-earliest --execute
```

### 8.4 Départ d'un consommateur

Terminal B : `Ctrl+C`. Puis, dans le terminal C, relancez plusieurs fois :

```bash
# [tools] terminal C
kafka-consumer-groups.sh --bootstrap-server $BS --describe --group analytics --state
kafka-consumer-groups.sh --bootstrap-server $BS --describe --group analytics --members --verbose
```

Terminal A : `Ctrl+C`.

### 8.5 Le nouveau protocole de groupe

```bash
# [tools] terminal A, puis la même commande dans le terminal B
kafka-console-consumer.sh --bootstrap-server $BS --topic orders --group analytics-v2 \
  --command-property group.protocol=consumer
```

```bash
# [tools] terminal C
kafka-consumer-groups.sh --bootstrap-server $BS --describe --group analytics-v2 --members --verbose
kafka-consumer-groups.sh --bootstrap-server $BS --list --type
```

Arrêtez les consommateurs des terminaux A et B (`Ctrl+C`), puis :

```bash
# [tools] terminal C
kafka-consumer-groups.sh --bootstrap-server $BS --describe --group analytics-v2 --state
```

### 8.6 Réinitialiser les offsets

```bash
# [tools]
kafka-consumer-groups.sh --bootstrap-server $BS --group billing --topic orders \
  --reset-offsets --to-earliest --dry-run
kafka-consumer-groups.sh --bootstrap-server $BS --group billing --topic orders \
  --reset-offsets --to-earliest --execute
kafka-consumer-groups.sh --bootstrap-server $BS --describe --group billing

kafka-consumer-groups.sh --bootstrap-server $BS --group billing --topic orders \
  --reset-offsets --to-latest --execute
kafka-consumer-groups.sh --bootstrap-server $BS --group billing --topic orders \
  --reset-offsets --shift-by -5 --execute

kafka-consumer-groups.sh --bootstrap-server $BS --group billing --topic orders \
  --reset-offsets --to-current --export --dry-run > /tmp/billing-offsets.csv
cat /tmp/billing-offsets.csv
```

### 8.7 Défi

À vous d'écrire les deux commandes : la simulation, puis l'application.

### 8.8 Rétention en temps

```bash
# [tools]
kafka-topics.sh --bootstrap-server $BS --create --topic audit-short --partitions 1 --replication-factor 3 \
  --config retention.ms=60000 --config segment.ms=10000
seq 1 100 | sed 's/^/audit-/' | kafka-console-producer.sh --bootstrap-server $BS --topic audit-short
kafka-get-offsets.sh --bootstrap-server $BS --topic audit-short --time earliest
kafka-get-offsets.sh --bootstrap-server $BS --topic audit-short --time latest
# 35 s : plus que segment.ms (10 s), et plus que la periode de verification de la retention (30 s dans le lab)
sleep 35; echo audit-101 | kafka-console-producer.sh --bootstrap-server $BS --topic audit-short
# Premier offset disponible, toutes les 10 s pendant 2 minutes
for i in $(seq 1 12); do
  echo "$(date +%T) $(kafka-get-offsets.sh --bootstrap-server $BS --topic audit-short --time earliest)"
  sleep 10
done
```

```bash
# [hôte]
docker compose exec kafka-1 ls -l /var/lib/kafka/data/audit-short-0
```

### 8.9 Compaction

```bash
# [tools]
kafka-topics.sh --bootstrap-server $BS --create --topic product-prices --partitions 1 --replication-factor 3 \
  --config cleanup.policy=compact --config segment.ms=10000 \
  --config min.cleanable.dirty.ratio=0.01 --config delete.retention.ms=1000
kafka-console-producer.sh --bootstrap-server $BS --topic product-prices \
  --reader-property parse.key=true --reader-property 'key.separator=|' < /opt/lab/data/product-prices.txt
sleep 12
echo 'P-500|9.99' | kafka-console-producer.sh --bootstrap-server $BS --topic product-prices \
  --reader-property parse.key=true --reader-property 'key.separator=|'
# Nombre de messages lisibles, toutes les 10 s pendant 2 minutes
for i in $(seq 1 12); do
  echo "$(date +%T) $(kafka-console-consumer.sh --bootstrap-server $BS --topic product-prices \
    --from-beginning --timeout-ms 4000 2>/dev/null | wc -l) messages"
  sleep 6
done
kafka-console-consumer.sh --bootstrap-server $BS --topic product-prices --from-beginning --timeout-ms 5000 \
  --formatter-property print.key=true --formatter-property print.offset=true
```

### 8.10 Tombstone

```bash
# [tools]
echo 'P-500|NULL' | kafka-console-producer.sh --bootstrap-server $BS --topic product-prices \
  --reader-property parse.key=true --reader-property 'key.separator=|' --reader-property null.marker=NULL
for i in 1 2 3 4 5; do
  sleep 15
  echo "P-90$i|0.9$i" | kafka-console-producer.sh --bootstrap-server $BS --topic product-prices \
    --reader-property parse.key=true --reader-property 'key.separator=|'
  echo "--- $(date +%T)"
  kafka-console-consumer.sh --bootstrap-server $BS --topic product-prices --from-beginning --timeout-ms 4000 \
    --formatter-property print.key=true --formatter-property print.offset=true 2>/dev/null | grep 'P-500'
done
```

## 9. Résultats attendus

### 9.1 Consommer avec un groupe

60 messages lus. Le groupe n'a plus de membre actif (`Consumer group 'billing' has no active members.`). Sur chaque partition, `CURRENT-OFFSET` est égal à `LOG-END-OFFSET` et le `LAG` vaut 0.

### 9.2 Lag

Toutes les commandes de `C008` vont dans la partition 5 : lag de 10 sur cette partition, 0 ailleurs. La relance lit exactement 10 messages, sans `--from-beginning`, car le groupe reprend à ses offsets validés.

### 9.3 Deux consommateurs

Chaque consommateur reçoit 3 partitions : `orders:0,1,2` et `orders:3,4,5`. État `Stable`, 2 membres, stratégie `range`. Le reset est refusé :

```
Error: Assignments can only be reset if the group 'analytics' is inactive, but the current state is Stable.
```

### 9.4 Départ d'un consommateur

En quelques secondes, l'état passe par `PreparingRebalance` puis revient à `Stable` avec 1 membre, qui reçoit les 6 partitions `orders:0,1,2,3,4,5`. Après l'arrêt du second : état `Empty`, 0 membre.

### 9.5 Nouveau protocole

Les colonnes `CURRENT-EPOCH` et `TARGET-EPOCH` sont renseignées. `--list --type` affiche `Consumer` pour `analytics-v2` et `Classic` pour les autres groupes. Après l'arrêt des consommateurs, la stratégie affichée est `uniform`.

### 9.6 Réinitialisation

- `--dry-run` affiche `NEW-OFFSET 0` partout, sans rien modifier.
- Après `--execute`, le lag total égale le nombre de messages du topic.
- `--shift-by -5` affiche deux avertissements `New offset (-5) is lower than earliest offset` pour les partitions vides 3 et 4.
- Le fichier CSV contient une ligne `topic,partition,offset` par partition.

### 9.7 Défi

La simulation affiche `NEW-OFFSET 0` sur toutes les partitions, avec deux avertissements pour les partitions vides : toutes les commandes du lab ont moins d'une heure.

### 9.8 Rétention

D'abord `audit-short:0:0` et `audit-short:0:100`. Entre 1 min et 1 min 40 après le 100e message, le premier offset passe à `audit-short:0:100` : dans le répertoire de la partition, l'ancien segment `00000000000000000000.log` a disparu. 30 à 60 s plus tard, il passe à `audit-short:0:101` : le segment actif, qui ne contient que `audit-101`, a expiré à son tour.

### 9.9 Compaction

21 messages juste après l'envoi de `P-500`, puis 5 en moins d'une minute : le dernier prix de `P-100`, `P-200`, `P-300`, `P-400` et `P-500`. Les offsets conservés ne se suivent pas.

### 9.10 Tombstone

- D'abord : la ligne `P-500	9.99` et le tombstone `P-500	null` coexistent.
- Ensuite : l'ancienne valeur `9.99` disparaît, le tombstone reste.
- Enfin, environ une minute après l'envoi : plus aucune ligne `P-500`.

## 10. Vérification

```bash
# [tools]
kafka-consumer-groups.sh --bootstrap-server $BS --list --type
kafka-get-offsets.sh --bootstrap-server $BS --topic audit-short --time earliest
kafka-console-consumer.sh --bootstrap-server $BS --topic product-prices --from-beginning --timeout-ms 5000 \
  --formatter-property print.key=true 2>/dev/null | cut -f1 | sort | uniq -d
```

| Contrôle | Attendu |
|---|---|
| Groupes | `billing`, `analytics` (Classic), `analytics-v2` (Consumer), `loadgen-consumers` en mode `extended` |
| Premier offset d'`audit-short` | 100, puis 101 une fois le segment actif expiré |
| Clés en double dans `product-prices` | aucune (la dernière commande n'affiche rien) |

## 11. Questions de réflexion

1. Pourquoi la relance de la facturation en 7.2 lit-elle 10 messages et non 70, alors qu'elle n'a pas `--from-beginning` ?
2. Que se passerait-il avec 8 consommateurs dans le groupe `analytics` ?
3. Pourquoi Kafka refuse-t-il de réinitialiser les offsets d'un groupe actif ? Comment procéder en production ?
4. Qu'apporte le nouveau protocole de groupe (KIP-848) lors d'un rééquilibrage ?
5. Pourquoi le message `audit-101` a-t-il survécu aux 100 premiers, alors que la rétention est la même pour tous ?
6. Un consommateur qui lit `product-prices` depuis le début voit-il toujours exactement un message par produit ?
7. Pourquoi un tombstone n'est-il pas supprimé immédiatement ?

## 12. Erreurs fréquentes

| Symptôme | Cause | Correction |
|---|---|---|
| `Assignments can only be reset if the group ... is inactive` | Un consommateur du groupe tourne encore, parfois dans un autre terminal | Arrêter tous les consommateurs du groupe, vérifier `--state` = `Empty` |
| Le reset ne change rien | `--dry-run` (ou aucune des deux options) au lieu de `--execute` | Ajouter `--execute` après avoir validé la simulation |
| Le nouveau consommateur ne lit rien | Groupe nouveau : il démarre à la fin (`auto.offset.reset=latest`) | `--from-beginning` pour un groupe sans offset validé |
| Un des deux consommateurs ne reçoit rien | Plus de consommateurs que de partitions, ou consommateurs dans des groupes différents | Vérifier le nom du groupe et `--members` |
| Le premier offset reste à 0 | Rien n'expire avant 60 s, puis la vérification de la rétention ne passe que toutes les 30 s | Attendre : jusqu'à 1 min 40 après le 100e message |
| Le premier offset passe directement de 0 à 101 | Les deux segments ont expiré avant la même vérification : 101e message parti moins de 30 s après les autres, ou dans le même segment (moins de 10 s) | Le constat reste juste (suppression par segment) ; pour voir l'étape à 100, recommencer sur un nouveau topic avec 35 s d'écart |
| La compaction ne réduit rien | Toutes les données sont dans le segment actif | Envoyer un message après `segment.ms` pour fermer le segment |
| Tombstone envoyé comme la chaîne `NULL` | `null.marker` oublié | `--reader-property null.marker=NULL` |
| `FencedInstanceIdException` sur un consommateur statique | Un autre consommateur avec le même `group.instance.id` s'est enregistré : le plus ancien est rejeté | Un identifiant distinct par instance |

## 13. Solution formateur

Voir `corrige.md`.

## 14. Variante avancée

1. Restaurez les offsets de `billing` à partir du fichier `/tmp/billing-offsets.csv` exporté en 7.6, après avoir déplacé le groupe à la fin du topic. Option : `--from-file`.
2. Trouvez la partition de `__consumer_offsets` qui stocke les offsets de `billing`, et vérifiez que son leader est le coordinateur affiché par `--state`. Indice : la partition est `abs(hashCode("billing")) % 50`, où `hashCode` est celui des chaînes Java : `h = 31 × h + code du caractère`, calculé sur 32 bits signés. L'image Kafka ne contient pas `jshell` : faites le calcul avec `awk`.
3. Lancez trois consommateurs dans `analytics` et arrêtez-en un brutalement (`kill -9` sur son processus Java) au lieu de `Ctrl+C`. Combien de temps le groupe met-il à réaffecter ses partitions ? Quel paramètre fixe ce délai ?
4. **Membres statiques.** Lancez deux consommateurs du groupe `statique` sur `orders`, chacun dans son terminal, avec `--consumer-property group.instance.id=inst-1` (puis `inst-2`) et l'assignor coopératif `--consumer-property partition.assignment.strategy=org.apache.kafka.clients.consumer.CooperativeStickyAssignor`. Relevez l'état du groupe et les partitions de chaque membre (`--describe --members --verbose` et `--state`). Arrêtez `inst-2` avec `Ctrl+C` : que devient le groupe au bout de 5 secondes ? Comparez avec l'étape 7.4. Relancez `inst-2` avec le même identifiant : qu'observez-vous ? Arrêtez-le de nouveau et attendez plus d'une minute : que devient le groupe, et quel paramètre fixe ce délai ?
