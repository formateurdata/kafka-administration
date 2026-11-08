# TP4 : garanties de livraison et panne de broker

| Repère | Valeur |
|---|---|
| Moment | Jour 1, 16:00-17:00, après la démo d'ouverture du TP (15:55-16:00) ; puis démonstration D4 par le formateur (17:00-17:10) |
| Compétences | C7 Paramètres de fiabilité ; C8 Panne de broker et réplication |
| Slides de rappel | S36 (ISR, LEO, HW), S37 (`min.insync.replicas` × `acks`), S38 (élection et ELR), S40 (panne contre perte de quorum) |
| Solution formateur | `corrige.md` (point 13) |
| Version notebook | `enonce.ipynb` dans JupyterLab (http://localhost:8888), mêmes étapes en Python ; démo d'ouverture : `demos-tp/demo-tp4.ipynb` |

Conventions : `# [hôte]` sur votre machine dans `lab/`, `# [tools]` dans `docker compose exec tools bash`. Les sections 7, 8 et 9 utilisent la même numérotation.

Mode conseillé : `extended`, pour voir les alertes à l'étape 7.7. En mode `core`, sautez cette étape.

---

## 1. Objectifs

À la fin du TP, vous savez :

- fixer le nombre minimal de répliques synchronisées au bon niveau (cluster, topic) ;
- prédire le résultat d'une production selon `acks` et `min.insync.replicas` quand un broker manque ;
- diagnostiquer une panne de broker : partitions sous-répliquées, ISR, ELR, quorum, brokers enregistrés ;
- vérifier qu'un producteur `acks=all` n'a perdu aucun message pendant la panne ;
- ramener le cluster à l'état nominal : rattrapage des répliques et élection des leaders préférés.

## 2. Durée

60 minutes de TP, après 5 minutes de démonstration d'ouverture ; puis 10 minutes de démonstration D4.

| Étape | Durée |
|---|---|
| 7.1 Fixer le minimum ISR du cluster | 6 min |
| 7.2 Créer `payments` et `refunds` | 4 min |
| 7.3 Produire en `acks=all` | 3 min |
| 7.4 Charge continue et arrêt de `kafka-3` | 4 min |
| 7.5 Diagnostiquer la panne | 10 min |
| 7.6 Prédire, puis produire | 8 min |
| 7.7 Alertes | 5 min |
| 7.8 Redémarrer `kafka-3` et revenir à l'état nominal | 10 min |
| 7.9 Défi : prouver qu'aucun paiement n'est perdu | 10 min |
| Démonstration D4 : perte de quorum (formateur) | 10 min |

## 3. Prérequis

- TP1 à TP3 terminés, cluster sain : `scripts/validate.sh` au vert.
- Notions : slides S35 à S41.

## 4. Architecture utilisée

```
  Avant la panne                          Pendant la panne (kafka-3 arrêté)

  kafka-1   kafka-2   kafka-3             kafka-1   kafka-2   kafka-3
   [L]       [F]       [F]   payments-1    [L]       [F]        X
   [F]       [L]       [F]   payments-2    [F]       [L]        X
   [F]       [F]       [L]   payments-0    [L]       [F]        X   <- nouveau leader élu

  ISR = {1,2,3}                           ISR = {1,2}  (2 >= min.insync.replicas = 2 : écriture acceptée)
  refunds : min.insync.replicas = 3       ISR = {1,2}  (2 <  3 : écriture acks=all refusée)

  Quorum KRaft : 3 votants. Avec 2 votants sur 3, la majorité est conservée.
```

| Topic | Partitions | RF | `min.insync.replicas` |
|---|---|---|---|
| `payments` | 3 | 3 | 2 (hérité du cluster) |
| `refunds` | 1 | 3 | 3 (posé sur le topic) |

## 5. Contexte métier

Pour NovaShop, un paiement perdu est un incident grave : le client est débité et la commande n'est jamais expédiée. La direction exige que le flux `payments` résiste à la perte d'un nœud, sans perte et sans interruption. Pour les remboursements, l'équipe Finance a demandé une confirmation par les 3 répliques.

Vous devez configurer ces exigences, puis les prouver en arrêtant un nœud en pleine charge.

## 6. Énoncé

1. Exigez au moins 2 répliques synchronisées par défaut sur tout le cluster.
2. Créez `payments` et `refunds` selon le tableau de la section 4.
3. Arrêtez `kafka-3` pendant qu'un producteur `acks=all` écrit 100 paiements par seconde.
4. Diagnostiquez l'état du cluster, prédisez puis vérifiez le comportement des productions selon `acks`.
5. Redémarrez `kafka-3`, ramenez le cluster à l'état nominal et prouvez qu'aucun paiement n'a été perdu.

## 7. Étapes

**7.1 Fixer le minimum ISR du cluster.** Affichez la configuration dynamique par défaut des brokers. Essayez de fixer `min.insync.replicas=2` sur le seul broker 1, puis pour tout le cluster.

**7.2 Créer `payments` et `refunds`.** Créez les deux topics, décrivez-les et vérifiez d'où vient la valeur de `min.insync.replicas` de `payments`.

**7.3 Produire en `acks=all`.** Envoyez 100 paiements en `acks=all`, puis affichez les offsets.

**7.4 Charge continue et arrêt de `kafka-3`.** Lancez en arrière-plan un producteur de 6000 paiements à 100 messages par seconde, en `acks=all`. Arrêtez `kafka-3` 5 secondes plus tard.

**7.5 Diagnostiquer la panne.** Mesurez :

- le nombre de partitions sous-répliquées ;
- les leaders et l'ISR de `payments` ;
- le nombre de partitions au minimum ISR, et celles sous le minimum ;
- l'état du quorum KRaft ;
- les brokers enregistrés.

**7.6 Prédire, puis produire.** Remplissez d'abord la colonne « Prédiction » du tableau, puis lancez les trois productions.

| Production | Prédiction | Résultat |
|---|---|---|
| 10 messages dans `payments`, `acks=all` | | |
| 1 message `REF-1` dans `refunds`, `acks=all` | | |
| 1 message `REF-2` dans `refunds`, `acks=1` | | |

**7.7 Alertes.** Dans Prometheus (http://localhost:9090, menu « Alerts »), relevez les alertes actives 1 à 2 minutes après l'arrêt.

**7.8 Redémarrer `kafka-3` et revenir à l'état nominal.** Redémarrez `kafka-3`. Mesurez le temps nécessaire pour que les partitions sous-répliquées reviennent à 0. Comparez les leaders de `payments` avec leurs leaders préférés, puis lancez une élection des leaders préférés.

**7.9 Défi : prouver qu'aucun paiement n'est perdu.** Combien de messages `payments` devrait-il contenir ? Prouvez-le avec deux sources indépendantes : les offsets du topic et le journal du producteur de charge. Vérifiez aussi le contenu de `refunds`.

## 8. Commandes directement exécutables

### 8.1 Fixer le minimum ISR du cluster

```bash
# [tools]
kafka-configs.sh --bootstrap-server $BS --describe --entity-type brokers --entity-default
kafka-configs.sh --bootstrap-server $BS --alter --entity-type brokers --entity-name 1 \
  --add-config min.insync.replicas=2
kafka-configs.sh --bootstrap-server $BS --alter --entity-type brokers --entity-default \
  --add-config min.insync.replicas=2
kafka-configs.sh --bootstrap-server $BS --describe --entity-type brokers --entity-default
```

### 8.2 Créer `payments` et `refunds`

```bash
# [tools]
kafka-topics.sh --bootstrap-server $BS --create --topic payments --partitions 3 --replication-factor 3
kafka-topics.sh --bootstrap-server $BS --create --topic refunds --partitions 1 --replication-factor 3 \
  --config min.insync.replicas=3
kafka-topics.sh --bootstrap-server $BS --describe --topic payments
kafka-topics.sh --bootstrap-server $BS --describe --topic refunds
kafka-configs.sh --bootstrap-server $BS --describe --entity-type topics --entity-name payments --all \
  | grep min.insync
```

### 8.3 Produire en `acks=all`

```bash
# [tools]
seq 1 100 | sed 's/^/PAY-/' | kafka-console-producer.sh --bootstrap-server $BS --topic payments \
  --request-required-acks all
kafka-get-offsets.sh --bootstrap-server $BS --topic payments
```

### 8.4 Charge continue et arrêt de `kafka-3`

```bash
# [hôte]
docker compose exec -d tools bash -c 'kafka-producer-perf-test.sh --bootstrap-server $BS --topic payments \
  --num-records 6000 --throughput 100 --record-size 200 --command-property acks=all \
  > /tmp/perf-payments.log 2>&1'
sleep 5
time docker compose stop kafka-3
```

### 8.5 Diagnostiquer la panne

Les avertissements `Couldn't resolve server kafka-3:19092` sont normaux : `$BS` contient encore l'adresse du nœud arrêté.

```bash
# [tools]
kafka-topics.sh --bootstrap-server $BS --describe --under-replicated-partitions | grep -c Partition
kafka-topics.sh --bootstrap-server $BS --describe --topic payments
kafka-topics.sh --bootstrap-server $BS --describe --at-min-isr-partitions | grep -c Partition
kafka-topics.sh --bootstrap-server $BS --describe --under-min-isr-partitions
kafka-metadata-quorum.sh --bootstrap-server $BS describe --replication
kafka-cluster.sh list-endpoints --bootstrap-server $BS
```

### 8.6 Prédire, puis produire

```bash
# [tools]
seq 101 110 | sed 's/^/PAY-/' | kafka-console-producer.sh --bootstrap-server $BS --topic payments \
  --request-required-acks all

echo REF-1 | kafka-console-producer.sh --bootstrap-server $BS --topic refunds --request-required-acks all \
  --command-property delivery.timeout.ms=15000 --command-property request.timeout.ms=5000

echo REF-2 | kafka-console-producer.sh --bootstrap-server $BS --topic refunds --request-required-acks 1

kafka-topics.sh --bootstrap-server $BS --describe --topic refunds
```

### 8.7 Alertes

Prometheus : http://localhost:9090/alerts. Équivalent en ligne de commande :

```bash
# [hôte]
curl -s localhost:9090/api/v1/alerts | grep -o '"alertname":"[A-Za-z]*"' | sort | uniq -c
```

### 8.8 Redémarrer `kafka-3`

```bash
# [hôte]
docker compose start kafka-3
```

```bash
# [tools]
# Partitions sous-répliquées toutes les 2 s, jusqu'à 0
while [ "$(kafka-topics.sh --bootstrap-server $BS --describe --under-replicated-partitions 2>/dev/null | grep -c Partition)" != 0 ]; do
  date +%T; sleep 2
done; echo "URP = 0 a $(date +%T)"
kafka-topics.sh --bootstrap-server $BS --describe --topic payments
kafka-leader-election.sh --bootstrap-server $BS --election-type preferred --all-topic-partitions
kafka-topics.sh --bootstrap-server $BS --describe --topic payments
```

### 8.9 Défi

À vous d'écrire les commandes. Indices : `kafka-get-offsets.sh` et `awk` pour le total ; `/tmp/perf-payments.log` dans le conteneur `tools`.

## 9. Résultats attendus

Les leaders, les nombres de partitions et les horodatages varient selon les topics présents sur votre cluster.

### 9.1 Minimum ISR du cluster

- Valeur de départ : `min.insync.replicas=1`, source `DYNAMIC_DEFAULT_BROKER_CONFIG`.
- Le réglage sur le seul broker 1 est refusé : `Broker-level min.insync.replicas cannot be altered while ELR is enabled.`
- Le réglage par défaut du cluster réussit : `Completed updating default config for brokers in the cluster.`

### 9.2 Topics

`payments` affiche `min.insync.replicas=2`, source `DYNAMIC_DEFAULT_BROKER_CONFIG`. `refunds` affiche `min.insync.replicas=3`.

### 9.3 Production

Sans clé, les 100 messages vont dans une seule partition, par exemple `payments:0:100`.

### 9.4 Arrêt

`docker compose stop kafka-3` dure environ 3 s.

### 9.5 Diagnostic

| Mesure | Résultat |
|---|---|
| Partitions sous-répliquées | plusieurs dizaines (toutes les partitions qui avaient une réplique sur `kafka-3`) |
| `payments` | aucun leader sur 3 ; `Isr: 1,2` ou `2,1` ; `Replicas` inchangé (`3,1,2`...) |
| Au minimum ISR | presque autant que les sous-répliquées |
| Sous le minimum ISR | `refunds`, avec `Isr: 2,1` et `Elr: 3` |
| Quorum | `kafka-3` toujours listé comme `Follower`, avec un `Lag` qui augmente ; un leader existe |
| Brokers enregistrés | 1 et 2 seulement |

### 9.6 Productions

| Production | Résultat |
|---|---|
| `payments`, `acks=all` | Réussite |
| `refunds`, `acks=all` | 3 tentatives `NOT_ENOUGH_REPLICAS`, puis `NotEnoughReplicasException: Messages are rejected since there are fewer in-sync replicas than required.` |
| `refunds`, `acks=1` | Réussite |

### 9.7 Alertes

Environ une minute après l'arrêt, 4 alertes à l'état `firing` : `KafkaNodeDown`, `KafkaUnderMinIsrPartitions`, `KafkaUnderReplicatedPartitions` et `KafkaFencedBrokers`.

### 9.8 Retour à l'état nominal

- Partitions sous-répliquées à 0 en quelques secondes après le démarrage.
- Avant l'élection, `payments-0` garde son leader de secours alors que son leader préféré est 3.
- `kafka-leader-election.sh` affiche `Successfully completed leader election (PREFERRED) for partitions ...`, puis 3 redevient leader de `payments-0`.

### 9.9 Défi

`payments` contient 100 + 10 + 6000 = 6110 messages. Le journal du producteur se termine par `6000 records sent` sans erreur fatale. `refunds` contient seulement `REF-2`.

## 10. Vérification

```bash
# [tools]
kafka-topics.sh --bootstrap-server $BS --describe --under-replicated-partitions | grep -c Partition
kafka-configs.sh --bootstrap-server $BS --describe --entity-type brokers --entity-default
kafka-get-offsets.sh --bootstrap-server $BS --topic payments | awk -F: '{s += $3} END {print s}'
```

| Contrôle | Attendu |
|---|---|
| Partitions sous-répliquées | 0 |
| `min.insync.replicas` du cluster | 2 |
| Messages dans `payments` | 6110 |
| Brokers enregistrés | 3 |

## 11. Questions de réflexion

1. Pourquoi le minimum ISR ne peut-il pas être fixé broker par broker quand l'ELR est actif ?
2. Avec RF 3 et `min.insync.replicas=2`, combien de brokers peut-on perdre sans bloquer les producteurs `acks=all` ? Et sans perdre de données ?
3. `refunds` avec `min.insync.replicas=3` : quel est le prix de cette exigence ? Que conseilleriez-vous à l'équipe Finance ?
4. Le message `REF-2` écrit en `acks=1` pendant la panne est-il en sécurité ? Pourquoi ?
5. Que représente `Elr: 3` sur `refunds` ?
6. Pourquoi faut-il une élection des leaders préférés après le retour d'un broker ? Que se passe-t-il si on ne la lance pas ?
7. Quelle différence entre perdre un broker (ce TP) et perdre le quorum (démonstration D4) ?

## 12. Erreurs fréquentes

| Symptôme | Cause | Correction |
|---|---|---|
| Nombreuses lignes `Couldn't resolve server kafka-3:19092` | `$BS` contient l'adresse du nœud arrêté | Normal pendant la panne ; ajouter `2>/dev/null` pour alléger |
| Commandes lentes (plusieurs secondes) | Le client essaie aussi `kafka-3` | Normal ; ou utiliser `--bootstrap-server kafka-1:19092` |
| La production `REF-1` attend 2 minutes | `delivery.timeout.ms` par défaut (120 s) | Garder les options `delivery.timeout.ms` et `request.timeout.ms` de la commande |
| Le total de `payments` dépasse 6110 | Étape 7.3 ou 7.6 lancée deux fois | Recalculer avec le nombre réel d'envois |
| `refunds` vide | `REF-2` envoyé avant l'arrêt de `kafka-3`, ou avec `acks=all` | Vérifier l'ordre des étapes |
| Les leaders ne reviennent pas sur 3 | Élection préférée non lancée | `kafka-leader-election.sh --election-type preferred` ; sinon le rééquilibrage automatique a lieu sous 5 min |

## 13. Solution formateur

Voir `corrige.md`.

## 14. Variante avancée

1. Recommencez la panne avec un arrêt brutal : `docker compose kill kafka-3` au lieu de `stop`. Mesurez, toutes les secondes, le temps pendant lequel `payments-0` garde `kafka-3` comme leader. Quel paramètre fixe ce délai ?
2. Au redémarrage, retrouvez dans les traces de `kafka-3` la preuve que l'arrêt n'a pas été propre.
3. Vérifiez sur `payments` que les 3 répliques de chaque partition sont dans 3 racks différents (`az-a`, `az-b`, `az-c`). Quel paramètre de broker le permet ?
