# TP6 : traces, métriques, alertes et diagnostic de lag

| Repère | Valeur |
|---|---|
| Moment | Jour 2, 11:30-12:30, après la démo d'ouverture du TP (11:25-11:30) |
| Compétences | C11 Traces log4j2 modifiées à chaud ; C12 Métriques, tableau de bord et alertes ; C13 Diagnostic d'un consumer lag |
| Slides de rappel | S53 (log4j2 à chaud), S54 et S55 (métriques broker et contrôleur), S57 (agent JMX), S58 (Prometheus et Grafana), S59 (alertes), S60 (lag) |
| Solution formateur | `corrige.md` (point 13) |
| Version notebook | `enonce.ipynb` dans JupyterLab (http://localhost:8888), mêmes étapes en Python ; démo d'ouverture : `demos-tp/demo-tp6.ipynb` |

Conventions : `# [hôte]` sur votre machine dans `lab/`, `# [tools]` dans `docker compose exec tools bash`. Les sections 7, 8 et 9 utilisent la même numérotation.

Gardez **deux terminaux** ouverts : A sur l'hôte, B dans `tools`. Restez dans la même session `tools` du début à la fin : les variables et fonctions définies en 8.2, 8.4 et 8.7 y sont réutilisées.

---

## 1. Objectifs

À la fin du TP, vous savez :

- retrouver où chaque famille de traces est écrite et avec quel niveau ;
- passer un logger en DEBUG sur un seul nœud, sans redémarrage, puis revenir au niveau normal ;
- lire les métriques brutes exposées par l'agent JMX d'un broker ;
- interroger Prometheus en PromQL et interpréter les indicateurs de santé ;
- lire le tableau de bord Grafana du cluster ;
- faire le ménage dans les consumer groups inactifs ;
- provoquer un lag, le suivre et voir une alerte passer de `pending` à `firing` ;
- écrire, valider et charger une règle d'alerte.

## 2. Durée

60 minutes, après 5 minutes de démonstration d'ouverture.

| Étape | Durée |
|---|---|
| 7.1 Où vont les traces | 5 min |
| 7.2 Changer un niveau de trace à chaud | 8 min |
| 7.3 Lire les métriques brutes | 6 min |
| 7.4 Interroger Prometheus | 8 min |
| 7.5 Lire le tableau de bord Grafana | 6 min |
| 7.6 Nettoyer les groupes inactifs | 4 min |
| 7.7 Provoquer et diagnostiquer un lag | 11 min |
| 7.8 Défi : écrire une alerte | 9 min |
| 7.9 Remettre le lab en état | 3 min |

## 3. Prérequis

- Lab démarré en mode `extended` (supervision et générateur de charge) : `scripts/start.sh extended` puis `scripts/validate.sh`. En mode `core`, Prometheus, Grafana et le générateur de charge n'existent pas : ce TP est impossible.
- TP du jour 1 et TP5 terminés. Les groupes `billing` et `billing-secure` existent.
- Navigateur sur l'hôte : Prometheus sur http://localhost:9090, Grafana sur http://localhost:3000.
- Notions : slides S52 à S60.

## 4. Architecture utilisée

```
  kafka-1..3 (JVM)                              loadgen-producer   loadgen-consumer
  MBeans -> agent JMX :9404/metrics             agent JMX client :9404/metrics
          |                                              |
          |            kafka-exporter :9308 (lag des groupes, lu dans le cluster)
          |                    |                         |
          +--------------------+-------------------------+
                               |  collecte toutes les 15 s
                               v
                Prometheus :9090  --  règles : monitoring/prometheus/rules/*.yml
                               |
                               v
                Grafana :3000  --  tableau de bord « Kafka - Cluster NovaShop (lab) »

  Traces : config/kafka/common/log4j2.yaml (monté sur chaque nœud)
           -> /opt/kafka/logs/server.log, controller.log, state-change.log,
              kafka-authorizer.log, log-cleaner.log, kafka-request.log
           niveau modifiable à chaud : kafka-configs.sh --entity-type broker-loggers
```

| Composant | Rôle dans le TP |
|---|---|
| `loadgen-producer` | Écrit 100 messages/s dans le topic `loadgen` |
| `loadgen-consumer` | Lit `loadgen` avec le groupe `loadgen-consumers` |
| `kafka-exporter` | Publie `kafka_consumergroup_lag`, calculé par le cluster |
| Règle `KafkaConsumerGroupLagHigh` | Lag d'un groupe sur un topic supérieur à 5000 pendant 1 minute (seuil de lab) |

## 5. Contexte métier

L'équipe Facturation de NovaShop signale des factures émises avec plusieurs minutes de retard. L'exploitation dispose d'une chaîne de supervision complète, mais personne ne l'a encore prise en main. Votre mission : savoir augmenter les traces du contrôleur pendant un incident, retrouver les bons indicateurs, et prouver qu'une alerte prévient l'astreinte quand un consommateur s'arrête.

## 6. Énoncé

1. Identifiez les fichiers de traces et le logger du contrôleur.
2. Passez ce logger en DEBUG sur le seul contrôleur actif, observez l'effet, puis revenez au niveau normal.
3. Retrouvez dans les métriques brutes, puis dans Prometheus et Grafana, les indicateurs de santé du cluster.
4. Supprimez les consumer groups qui ne servent plus.
5. Arrêtez le consommateur du générateur de charge, suivez le lag et l'alerte, puis rétablissez le service.
6. Écrivez une alerte qui détecte l'arrêt du producteur, prouvez qu'elle fonctionne, puis retirez-la.

## 7. Étapes

**7.1 Où vont les traces.** Dans `config/kafka/common/log4j2.yaml`, relevez la fréquence de relecture du fichier, les fichiers de traces produits, et la configuration du logger `org.apache.kafka.controller`. Listez les fichiers présents dans `/opt/kafka/logs` d'un nœud.

**7.2 Changer un niveau de trace à chaud.** Trouvez le contrôleur actif. Comptez les loggers configurables sur ce nœud et relevez le niveau de `org.apache.kafka.controller`. Passez-le en DEBUG. Essayez aussi le nom `kafka.controller`, courant dans les documentations 3.x. Créez le topic `inventory` (3 partitions, compacté). Comptez les lignes DEBUG de `controller.log` à 10 s d'intervalle, puis retrouvez la création du topic dans ce fichier. Revenez au niveau par défaut et vérifiez que les lignes DEBUG s'arrêtent.

**7.3 Lire les métriques brutes.** Interrogez l'agent JMX de `kafka-1` : nombre de partitions, de leaders, de partitions sous-répliquées. Affichez `activecontrollercount` sur les 3 nœuds. Comptez les séries exposées par un nœud.

**7.4 Interroger Prometheus.** Définissez la fonction `q`, puis vérifiez : un seul contrôleur actif, aucune partition sous-répliquée, le débit entrant par topic, l'inactivité des threads de requêtes, le lag par groupe, l'état des cibles collectées. Ouvrez ensuite http://localhost:9090 et refaites une requête dans l'onglet Graph.

**7.5 Lire le tableau de bord Grafana.** Ouvrez http://localhost:3000, puis le tableau de bord « Kafka - Cluster NovaShop (lab) ». Relevez les 8 valeurs de la rangée « Sante du cluster ». Retrouvez le débit du topic `loadgen` et la répartition des leaders par broker. Comparez avec les étapes 7.3 et 7.4.

**7.6 Nettoyer les groupes inactifs.** Listez les groupes avec leur état. Essayez de supprimer `loadgen-consumers`. Supprimez `analytics`, `analytics-v2` et tous les groupes `console-consumer-*`. Conservez `billing` et `billing-secure`.

**7.7 Provoquer et diagnostiquer un lag.** Dans le terminal B, définissez les fonctions de suivi et lancez la boucle d'observation. Dans le terminal A, arrêtez `loadgen-consumer`. Notez l'heure où l'alerte `KafkaConsumerGroupLagHigh` passe en `pending`, puis en `firing`. Décrivez le groupe pendant l'arrêt. Redémarrez le consommateur et mesurez le temps de rattrapage.

**7.8 Défi : écrire une alerte.** Créez `monitoring/prometheus/rules/tp6-custom.yml` à partir du modèle de la section 8.8. L'alerte `KafkaLoadgenNoTraffic` doit se déclencher quand le topic `loadgen` reçoit moins de 10 messages par seconde pendant 1 minute. Validez le fichier avec `promtool`, rechargez Prometheus, puis prouvez que l'alerte se déclenche en arrêtant `loadgen-producer`. Redémarrez-le.

**7.9 Remettre le lab en état.** Supprimez `tp6-custom.yml`, rechargez Prometheus, et vérifiez que les deux clients du générateur tournent, que le logger du contrôleur est revenu à INFO et qu'aucune alerte n'est active.

## 8. Commandes directement exécutables

### 8.1 Traces

```bash
# [hôte]
grep -nE 'monitorInterval|fileName' config/kafka/common/log4j2.yaml
grep -n -A5 'name: org.apache.kafka.controller' config/kafka/common/log4j2.yaml
docker compose exec kafka-1 ls /opt/kafka/logs
```

### 8.2 Niveau de trace à chaud

```bash
# [tools] terminal B
L=$(kafka-metadata-quorum.sh --bootstrap-server $BS describe --status | awk '/LeaderId/ {print $2}')
echo "Contrôleur actif : $L"

kafka-configs.sh --bootstrap-server $BS --describe --entity-type broker-loggers --entity-name $L | grep -c '='
kafka-configs.sh --bootstrap-server $BS --describe --entity-type broker-loggers --entity-name $L \
  | grep -E ' (org.apache.kafka.controller|root)='

kafka-configs.sh --bootstrap-server $BS --alter --entity-type broker-loggers --entity-name $L \
  --add-config org.apache.kafka.controller=DEBUG
kafka-configs.sh --bootstrap-server $BS --alter --entity-type broker-loggers --entity-name $L \
  --add-config kafka.controller=DEBUG

kafka-topics.sh --bootstrap-server $BS --create --topic inventory --partitions 3 --replication-factor 3 \
  --config cleanup.policy=compact
```

```bash
# [hôte] terminal A : remplacez kafka-2 par kafka-<numéro du contrôleur actif>
docker compose exec kafka-2 sh -c 'grep -c DEBUG /opt/kafka/logs/controller.log'
sleep 10
docker compose exec kafka-2 sh -c 'grep -c DEBUG /opt/kafka/logs/controller.log'
docker compose exec kafka-2 sh -c 'grep DEBUG /opt/kafka/logs/controller.log | tail -3'
docker compose exec kafka-2 sh -c 'grep inventory /opt/kafka/logs/controller.log' | cut -c1-200
```

```bash
# [tools] terminal B : retour au niveau par défaut
kafka-configs.sh --bootstrap-server $BS --alter --entity-type broker-loggers --entity-name $L \
  --delete-config org.apache.kafka.controller
kafka-configs.sh --bootstrap-server $BS --describe --entity-type broker-loggers --entity-name $L \
  | grep ' org.apache.kafka.controller='
```

```bash
# [hôte] terminal A : le compteur ne doit plus bouger
docker compose exec kafka-2 sh -c 'grep -c DEBUG /opt/kafka/logs/controller.log'
sleep 10
docker compose exec kafka-2 sh -c 'grep -c DEBUG /opt/kafka/logs/controller.log'
```

### 8.3 Métriques brutes

```bash
# [tools]
wget -qO- http://kafka-1:9404/metrics \
  | grep -E '^kafka_server_replicamanager_(underreplicatedpartitions|leadercount|partitioncount)'
for n in 1 2 3; do
  echo "kafka-$n : $(wget -qO- http://kafka-$n:9404/metrics | grep '^kafka_controller_kafkacontroller_activecontrollercount')"
done
wget -qO- http://kafka-1:9404/metrics | grep -vc '^#'
```

### 8.4 PromQL

```bash
# [tools]
q() { wget -qO- --post-data "query=$1" http://prometheus:9090/api/v1/query; echo; }

q 'sum(kafka_controller_kafkacontroller_activecontrollercount)'
q 'sum(kafka_server_replicamanager_underreplicatedpartitions)'
q 'sum by (topic) (rate(kafka_server_brokertopicmetrics_messagesin_total{topic!=""}[1m]))'
q 'kafka_server_kafkarequesthandlerpool_requesthandleravgidlepercent'
q 'sum by (consumergroup, topic) (kafka_consumergroup_lag)'
q 'up'
```

Dans le navigateur : http://localhost:9090, onglet Graph, requête `sum by (topic) (rate(kafka_server_brokertopicmetrics_messagesin_total{topic!=""}[1m]))`, vue Graph sur 15 minutes. Consultez aussi les menus Status > Targets et Alerts.

### 8.5 Grafana

Ouvrez http://localhost:3000. La consultation est ouverte sans compte (rôle Viewer). Pour modifier un panneau, connectez-vous avec `admin` / `kafka-lab`. Menu Dashboards, puis « Kafka - Cluster NovaShop (lab) ». Choisissez la période « Last 15 minutes ».

### 8.6 Groupes inactifs

```bash
# [tools]
kafka-consumer-groups.sh --bootstrap-server $BS --list --state
kafka-consumer-groups.sh --bootstrap-server $BS --delete --group loadgen-consumers

kafka-consumer-groups.sh --bootstrap-server $BS --delete --group analytics --group analytics-v2
for g in $(kafka-consumer-groups.sh --bootstrap-server $BS --list | grep '^console-consumer-'); do
  kafka-consumer-groups.sh --bootstrap-server $BS --delete --group $g
done
kafka-consumer-groups.sh --bootstrap-server $BS --list --state
```

### 8.7 Lag

```bash
# [tools] terminal B : fonctions de suivi
lag() {
  kafka-consumer-groups.sh --bootstrap-server $BS --describe --group loadgen-consumers 2>/dev/null \
    | awk '$1=="loadgen-consumers" {s+=$6} END {print s}'
}
alerts() {
  wget -qO- http://prometheus:9090/api/v1/alerts | grep -oE '"(alertname|state)":"[^"]+"' \
    | cut -d'"' -f4 | paste -d: - - | sort -u | paste -sd' ' -
}
echo "lag=$(lag) alertes=[$(alerts)]"
```

```bash
# [tools] terminal B : boucle d'observation (environ 3 min 30), lancez-la puis passez au terminal A
for i in $(seq 1 12); do echo "$(date +%T) lag=$(lag) alertes=[$(alerts)]"; sleep 15; done
```

```bash
# [hôte] terminal A
docker compose stop loadgen-consumer
```

```bash
# [tools] terminal B, une fois l'alerte en firing
kafka-consumer-groups.sh --bootstrap-server $BS --describe --group loadgen-consumers
q 'up{instance="loadgen-consumer"}'
q 'count(kafka_consumer_consumer_fetch_manager_metrics_records_lag_max)'
```

```bash
# [hôte] terminal A
docker compose start loadgen-consumer
```

```bash
# [tools] terminal B : rattrapage
for i in $(seq 1 6); do echo "$(date +%T) lag=$(lag) alertes=[$(alerts)]"; sleep 15; done
```

### 8.8 Défi

Modèle à recopier dans `monitoring/prometheus/rules/tp6-custom.yml`, en remplaçant les valeurs entre `< >` :

```yaml
groups:
  - name: tp6-custom
    rules:
      - alert: KafkaLoadgenNoTraffic
        expr: <requête PromQL : messages entrants par seconde sur le topic loadgen, tous brokers confondus, inférieurs à 10>
        for: <durée>
        labels:
          severity: warning
        annotations:
          summary: "Le topic loadgen recoit moins de 10 messages/s depuis 1 minute"
```

```bash
# [hôte] terminal A : validation, rechargement, contrôle de chargement
docker compose exec prometheus promtool check rules /etc/prometheus/rules/tp6-custom.yml
curl -s -w 'HTTP %{http_code}\n' -X POST localhost:9090/-/reload
curl -s localhost:9090/api/v1/rules | grep -o '"name":"KafkaLoadgenNoTraffic"'
docker compose stop loadgen-producer
```

```bash
# [tools] terminal B
for i in $(seq 1 10); do echo "$(date +%T) alertes=[$(alerts)]"; sleep 15; done
```

```bash
# [hôte] terminal A, une fois l'alerte en firing
docker compose start loadgen-producer
```

### 8.9 Remise en état

```bash
# [hôte]
rm monitoring/prometheus/rules/tp6-custom.yml
curl -s -w 'HTTP %{http_code}\n' -X POST localhost:9090/-/reload
docker compose ps loadgen-producer loadgen-consumer
```

```bash
# [tools] attendez 1 à 2 minutes après le redémarrage du producteur
kafka-configs.sh --bootstrap-server $BS --describe --entity-type broker-loggers --entity-name $L \
  | grep ' org.apache.kafka.controller='
echo "lag=$(lag) alertes=[$(alerts)]"
```

## 9. Résultats attendus

### 9.1 Traces

- `monitorInterval: 30` : le fichier est relu toutes les 30 s.
- 6 fichiers déclarés : `server.log`, `state-change.log`, `kafka-request.log`, `log-cleaner.log`, `controller.log`, `kafka-authorizer.log`.
- `org.apache.kafka.controller` : niveau `INFO`, `additivity: false`, appender `ControllerAppender`. Ses lignes vont donc dans `controller.log` uniquement.
- Les fichiers tournent toutes les heures : `controller.log.2026-10-27-21` contient l'heure précédente. On trouve aussi les traces du ramasse-miettes (`kafkaServer-gc.log`).

### 9.2 Niveau de trace à chaud

- Environ 200 loggers configurables sur le nœud (206 sur le lab de référence). `org.apache.kafka.controller` et `root` sont en `INFO`.
- Le passage en DEBUG répond `Completed updating config for broker-logger <n>.`
- `kafka.controller` est refusé : `Invalid broker logger(s): kafka.controller`.
- Le compteur DEBUG augmente d'environ 200 lignes en 10 s. Exemple de ligne :

  ```
  DEBUG [QuorumController id=2] Processed handleCommit[baseOffset=2787] in 217 us (org.apache.kafka.controller.QuorumController)
  ```

- La création d'`inventory` apparaît en INFO, dans l'ordre : `CreateTopics result(s): CreatableTopic(name='inventory', numPartitions=3, replicationFactor=3, ...): SUCCESS`, puis `Replayed TopicRecord`, `Replayed ConfigRecord` (`cleanup.policy` à `compact`) et 3 `Replayed PartitionRecord`.
- Après `--delete-config`, le logger repasse en `INFO` et le compteur DEBUG ne bouge plus.

### 9.3 Métriques brutes

```
kafka_server_replicamanager_leadercount 42.0
kafka_server_replicamanager_partitioncount 80.0
kafka_server_replicamanager_underreplicatedpartitions 0.0
```

Les nombres de partitions et de leaders dépendent des topics créés. `activecontrollercount` vaut `1.0` sur le contrôleur actif trouvé en 7.2 et `0.0` sur les deux autres. Un nœud expose environ 6600 séries.

### 9.4 PromQL

| Requête | Valeur attendue |
|---|---|
| Somme des contrôleurs actifs | `"1"` |
| Somme des partitions sous-répliquées | `"0"` |
| Débit par topic | `loadgen` vers 100 messages/s, environ 1 pour `__consumer_offsets`, 0 pour les topics inactifs |
| Inactivité des threads | Environ 0,998 sur chaque nœud |
| Lag par groupe | Quelques centaines au plus pour `loadgen-consumers`, valeurs fixes pour les groupes arrêtés |
| `up` | `1` pour toutes les cibles, sauf `kafka-4` (job `kafka-scale`) à `0` |

### 9.5 Grafana

| Panneau de la rangée « Sante du cluster » | Valeur |
|---|---|
| Noeuds joignables | 3 |
| Controleur actif | 1 |
| Leader du quorum (node.id) | Le numéro trouvé en 7.2 |
| Brokers actifs | 3 |
| Brokers isoles (fenced) | 0 |
| Partitions sous-repliquees | 0 |
| Sous min ISR | 0 |
| Partitions offline | 0 |

Le panneau « Messages entrants / s par topic » montre `loadgen` vers 100. « Leaders par broker » montre une répartition à peu près égale entre les 3 brokers.

### 9.6 Groupes

- `loadgen-consumers` est `Stable`. Sa suppression est refusée : `GroupNotEmptyException: The group is not empty.`
- `Deletion of requested consumer groups ('analytics', 'analytics-v2') was successful.`
- La liste finale contient `loadgen-consumers`, `billing` et `billing-secure`.

### 9.7 Lag

| Temps après l'arrêt | Lag | Alerte `KafkaConsumerGroupLagHigh` |
|---|---|---|
| 15 s | Quelques centaines | Aucune |
| 1 min | Environ 5000 | Aucune ou `pending` |
| 1 min 15 à 1 min 30 | Environ 6000 | `pending` |
| 2 min à 2 min 30 | Environ 12000 à 15000 | `firing` |

- Le lag augmente de 1500 toutes les 15 s, soit le débit du producteur.
- Pendant l'arrêt, le groupe n'a plus de membre : `Consumer group 'loadgen-consumers' has no active members.` La colonne `CONSUMER-ID` affiche `-`.
- `up{instance="loadgen-consumer"}` vaut `0`, et la requête sur `records_lag_max` ne renvoie plus rien.
- Après le redémarrage, le lag retombe sous 1000 en 30 s environ, et l'alerte disparaît.

### 9.8 Défi

```
Checking /etc/prometheus/rules/tp6-custom.yml
  SUCCESS: 1 rules found
HTTP 200
"name":"KafkaLoadgenNoTraffic"
```

Après l'arrêt du producteur : alerte absente pendant environ 45 s, puis `KafkaLoadgenNoTraffic:pending` vers 1 min, puis `firing` vers 2 min.

### 9.9 Remise en état

`HTTP 200`, deux conteneurs `Up`, logger en `INFO`, lag de quelques centaines au plus et `alertes=[]`.

## 10. Vérification

| Contrôle | Commande | Attendu |
|---|---|---|
| Logger du contrôleur | `kafka-configs.sh ... --describe --entity-type broker-loggers --entity-name $L` | `org.apache.kafka.controller=INFO` |
| Topic `inventory` | `kafka-topics.sh --bootstrap-server $BS --describe --topic inventory` | 3 partitions, `cleanup.policy=compact` |
| Groupes | `kafka-consumer-groups.sh --bootstrap-server $BS --list --state` | Ni `analytics` ni `console-consumer-*` |
| Règles | `ls monitoring/prometheus/rules/` (hôte) | `kafka-alerts.yml` seul |
| Alertes | `alerts` | Vide |
| Débit | `q 'sum(rate(kafka_server_brokertopicmetrics_messagesin_total{topic="loadgen"}[1m]))'` | Environ 100 |

Notez dans votre compte rendu les trois heures relevées en 7.7 : arrêt, passage en `pending`, passage en `firing`.

## 11. Questions de réflexion

1. Pourquoi passer le logger en DEBUG sur le seul contrôleur actif ? Que perdez-vous si le leadership du quorum change pendant l'incident ?
2. Le niveau modifié par `broker-loggers` survit-il au redémarrage du nœud ? Et une modification de `log4j2.yaml` ?
3. Que signifie une somme de `activecontrollercount` égale à 0 ? Égale à 2 ?
4. `kafka-4` est à `up=0`. Pourquoi n'est-ce pas une alerte ?
5. `requesthandleravgidlepercent` vaut 0,998. Que signifierait une valeur de 0,2, et quelle alerte se déclencherait ?
6. Le lag dépasse 5000 environ 1 minute après l'arrêt, mais l'alerte ne passe en `firing` qu'une minute plus tard. Quel compromis règle la durée `for` ?
7. Le consommateur arrêté, sa métrique `records_lag_max` disparaît. Pourquoi le lag doit-il être mesuré côté cluster ?
8. Un lag de 5000 messages est-il grave ? Comment l'exprimer en temps de retard ?
9. Quel risque prenez-vous en supprimant un groupe vide qui appartient à une application simplement arrêtée ?

## 12. Erreurs fréquentes

| Symptôme | Cause | Correction |
|---|---|---|
| `Invalid broker logger(s): kafka.controller` | Nom de logger 3.x ; en 4.x, les loggers portent le nom des classes | `org.apache.kafka.controller` ; liste complète par `--describe --entity-type broker-loggers` |
| Après `--delete-config`, un logger affiche INFO alors que le fichier le règle à WARN | `--delete-config` applique le niveau du logger `root`, pas celui du fichier | `--add-config <logger>=WARN`, ou redémarrage du nœud |
| Aucune ligne DEBUG dans `controller.log` | Logger modifié sur un nœud qui n'est pas le contrôleur actif, ou `grep` sur un autre nœud | Relire `LeaderId` ; même numéro pour `--entity-name` et `kafka-<n>` |
| `wget: bad address 'prometheus'` | Lab démarré en mode `core` | `scripts/start.sh extended` |
| `"result":[]` | Faute dans le nom de la métrique ou dans un label | Chercher le nom exact dans `:9404/metrics` |
| `lag()` affiche une ligne vide | Fonction définie dans une autre session `tools` | Redéfinir les fonctions de 8.7 dans la session courante |
| Panneaux Grafana vides | Période trop ancienne, ou mode `core` | Période « Last 15 minutes » ; vérifier les cibles dans Prometheus |
| Alerte jamais `pending` | Expression fausse, ou règle non chargée | `curl -s localhost:9090/api/v1/rules`, menu Status > Rules |
| `promtool` signale une erreur YAML | Indentation, ou tabulations | Espaces uniquement, en suivant le modèle |
| `GroupNotEmptyException` à la suppression | Le groupe a encore des membres | Arrêter les consommateurs, ou ne pas supprimer ce groupe |
| Le lag ne diminue pas après `start` | Le consommateur n'est pas reparti | `docker compose ps loadgen-consumer` puis `docker compose logs --tail 20 loadgen-consumer` |

## 13. Solution formateur

Voir `corrige.md`.

## 14. Variante avancée

1. **Lag en secondes.** Écrivez une requête PromQL qui exprime le retard de `loadgen-consumers` en secondes : le lag divisé par le débit entrant du topic. Que vaut-elle en temps normal ? Que vaudrait-elle avec le même lag et un débit dix fois plus faible ?
2. **Métriques client contre métriques cluster.** Comparez `kafka_consumer_consumer_fetch_manager_metrics_records_lag_max` (vue du client) et `kafka_consumergroup_lag` (vue du cluster) en temps normal. Pourquoi la valeur client peut-elle être 0 quand le lag du groupe ne l'est pas ?
3. **Démo D5 en autonomie.** Dans `config/kafka/common/log4j2.yaml`, passez `kafka.request.logger` de `WARN` à `DEBUG`. Mesurez, pour chacun des 3 nœuds, le délai avant que `--describe --entity-type broker-loggers` affiche ce niveau, et regardez grossir `kafka-request.log`. Restaurez le fichier d'origine avec `cp config/kafka/pristine/log4j2.yaml config/kafka/common/log4j2.yaml`, puis vérifiez que le fichier de traces cesse de grossir. Pourquoi ce logger ne doit-il jamais rester en DEBUG en production ?
4. **Partitions à la limite du minimum ISR.** Créez un topic `atmin` (3 partitions, RF 3, `min.insync.replicas=2`). Écrivez la requête PromQL qui compte ses partitions dont l'ISR est exactement égale au minimum (métrique `kafka_cluster_partition_atminisr`, une série par partition). Quelle est sa valeur ? Arrêtez `kafka-3`, attendez 50 secondes, relisez-la, et comparez avec le nombre de partitions sous le minimum ISR. Les écritures `acks=all` sont-elles encore acceptées ? Pourquoi cette métrique est-elle un meilleur signal d'alerte que le seul « sous le minimum » ? Relancez `kafka-3` ensuite.
