# TP8 : atelier incidents en binômes

| Repère | Valeur |
|---|---|
| Moment | Jour 2, 15:35-16:10, après la présentation de la méthode (S70 à S72) et la démo d'ouverture du TP (15:30-15:35) |
| Compétences | C9 Listeners, TLS et SASL ; C13 Diagnostic ; C16 Méthode de diagnostic d'incident |
| Slides de rappel | S70 (méthode), S71 (arbre de décision et messages d'erreur), S72 (boîte à outils) |
| Fiche à remplir | `fiche-diagnostic.md`, une par incident |
| Solution formateur | `corrige.md` (point 13) |
| Version notebook | `enonce.ipynb` dans JupyterLab (http://localhost:8888), mêmes étapes en Python ; démo d'ouverture : `demos-tp/demo-tp8.ipynb` |

Conventions : `# [hôte]` sur votre machine dans `lab/`, `# [tools]` dans `docker compose exec tools bash`. Les sections 7, 8 et 9 utilisent la même numérotation.

**Règle du jeu** : ne lisez pas `scripts/incident.sh`, et n'utilisez pas ses commandes `status` : elles donnent la réponse. Le diagnostic se fait avec la boîte à outils de la section 8.3, comme en production.

---

## 1. Objectifs

À la fin du TP, vous savez :

- qualifier un incident à partir d'un ticket et d'un message d'erreur ;
- observer le cluster du général au particulier, avant d'agir ;
- formuler une hypothèse et la vérifier par une commande ;
- corriger par l'action la moins risquée, puis prouver le retour à la normale ;
- rédiger une fiche d'incident exploitable par une autre équipe.

## 2. Durée

35 minutes, après 5 minutes de démonstration d'ouverture.

| Étape | Durée |
|---|---|
| 7.1 Préparer le binôme et le lab | 5 min |
| 7.2 Tour 1 | 10 min |
| 7.3 Tour 2 | 10 min |
| 7.4 Tour 3 | 10 min |

Dans chaque tour : 1 minute d'injection, 7 minutes de diagnostic et de correction, 1 minute de validation, 1 minute d'échange entre les deux membres du binôme.

## 3. Prérequis

- Lab en mode `extended`, sain : `scripts/validate.sh` sans échec. Les incidents utilisent le générateur de charge et Prometheus.
- TP5 et TP6 terminés : listener SECURE, comptes SCRAM, requêtes Prometheus et alertes.
- Une copie de `fiche-diagnostic.md` par incident.
- Notions : slides S70 à S72, et S71 en particulier pour les messages d'erreur.

## 4. Architecture utilisée

```
  Binôme : stagiaire A (lab A) et stagiaire B (lab B)

  Tour n :  B injecte un incident sur le lab A  --->  A diagnostique et corrige sur son lab
            A injecte un incident sur le lab B  --->  B diagnostique et corrige sur son lab
            (en parallèle, puis échange de fiches)

  Lab : kafka-1..3 (broker et contrôleur), listeners INTERNAL :19092 et SECURE :19094 (SASL_SSL),
        loadgen 100 messages/s, Prometheus :9090 et ses alertes, Grafana :3000
```

L'injecteur connaît l'incident et joue l'équipe qui a ouvert le ticket. Il répond aux questions sur le contexte, jamais sur la cause.

## 5. Contexte métier

Fin d'après-midi chez NovaShop. Trois tickets arrivent à l'équipe d'exploitation Kafka, dont vous assurez l'astreinte. Chacun doit être traité avec méthode : une correction hasardeuse sur le cluster de production coûterait plus cher que l'incident lui-même.

## 6. Énoncé

Chaque stagiaire traite les trois tickets ci-dessous, sur son propre lab, dans l'ordre fixé par le tableau de la section 7.1. Pour chacun : diagnostic, correction, validation et fiche remplie.

| Ticket | Équipe | Texte du ticket | Objectif de résolution |
|---|---|---|---|
| **Logistique** | Logistique | « Notre nouveau service `shipping-service` doit publier ses événements d'expédition dans le topic `shipments`, sur le listener sécurisé. On nous a créé le compte `shipping` et on a livré notre fichier de configuration. L'envoi échoue. Notre script de test : `/tmp/incident4/send-shipment.sh` dans `tools`. » | Le script se termine sans erreur et l'événement est lu dans `shipments`. |
| **Paiements** | Paiements | « Depuis une intervention sur `kafka-3`, une partie des paiements n'est plus enregistrée : certains envois expirent, d'autres passent. Les alertes de réplication se sont allumées. » | Tous les envois passent ; URP et partitions sous min ISR à 0 ; alertes levées. |
| **Stockage** | Supervision | « L'alerte `KafkaBrokerLogSizeHigh` s'est déclenchée sur les 3 nœuds. L'équipe Data a lancé une collecte de `clickstream` cet après-midi. Sa politique : garder au plus 1 heure et 5 Mo par partition. » | Volume revenu sous le seuil, limite en place contre la récidive, sans supprimer le topic ; alerte levée. |

## 7. Étapes

**7.1 Préparer le binôme et le lab.** Vérifiez que votre lab est sain. Définissez les fonctions de la boîte à outils dans votre session `tools` et lancez un premier bilan : c'est votre référence « tout va bien ». Fixez l'ordre des tickets :

| Tour | Lab de A (injecté par B) | Lab de B (injecté par A) |
|---|---|---|
| 1 | Paiements | Logistique |
| 2 | Stockage | Paiements |
| 3 | Logistique | Stockage |

Correspondance, **réservée à l'injecteur** : Logistique = 4, Paiements = 5, Stockage = 6.

**7.2, 7.3 et 7.4 : un tour.** Chaque tour suit les mêmes étapes :

1. L'injecteur lance l'incident sur le lab de son partenaire, attend le message `[ OK ]`, puis lui lit le ticket.
2. Pour le ticket Paiements, attendez 30 s avant la première observation. Pour le ticket Stockage, attendez l'alerte : 1 min 30 à 2 min selon le volume déjà présent sur votre lab.
3. Reproduisez le symptôme avec la commande de reproduction du ticket (section 8.2).
4. Qualifiez, observez, formulez une hypothèse, vérifiez-la, puis corrigez. Notez tout dans la fiche, avec les heures.
5. Validez avec les preuves de la section 9.
6. L'injecteur lance la remise en état du lab (section 8.4), puis les deux membres échangent leurs fiches.

## 8. Commandes directement exécutables

### 8.1 Préparation

```bash
# [hôte]
scripts/validate.sh
scripts/incident.sh list
```

```bash
# [tools] fonctions de la boîte à outils (à redéfinir dans chaque nouvelle session tools)
q() { wget -qO- --post-data "query=$1" http://prometheus:9090/api/v1/query; echo; }
alertes() {
  wget -qO- http://prometheus:9090/api/v1/alerts | grep -oE '"(alertname|state)":"[^"]+"' \
    | cut -d'"' -f4 | paste -d: - - | sort -u | paste -sd' ' -
}
bilan() {
  echo "== $(date +%T) quorum"
  kafka-metadata-quorum.sh --bootstrap-server $BS describe --status | grep -E 'LeaderId|MaxFollowerLag:'
  echo "== brokers"
  kafka-cluster.sh list-endpoints --bootstrap-server $BS
  echo "== URP : $(kafka-topics.sh --bootstrap-server $BS --describe --under-replicated-partitions | grep -c Partition)" \
       " sous min ISR : $(kafka-topics.sh --bootstrap-server $BS --describe --under-min-isr-partitions | grep -c Partition)" \
       " indisponibles : $(kafka-topics.sh --bootstrap-server $BS --describe --unavailable-partitions | grep -c Partition)"
  echo "== alertes : [$(alertes)]"
}
bilan
```

### 8.2 Injection et reproduction

```bash
# [hôte] par l'injecteur, sur le poste de son partenaire (N = 4, 5 ou 6)
scripts/incident.sh N inject
```

Reproduction du symptôme, par celui qui diagnostique :

| Ticket | Commande de reproduction |
|---|---|
| Logistique | `/tmp/incident4/send-shipment.sh` dans `tools` |
| Paiements | Les 6 envois ci-dessous, dans `tools` |
| Stockage | `alertes` dans `tools`, et le panneau « Donnees stockees par broker » de Grafana |

```bash
# [tools] ticket Paiements : 6 paiements, un par clé
for k in PAY-9001 PAY-9002 PAY-9003 PAY-9004 PAY-9005 PAY-9006; do echo "$k|{\"amount\":42}"; done \
  | kafka-console-producer.sh --bootstrap-server $BS --topic payments \
      --reader-property parse.key=true --reader-property 'key.separator=|' \
      --command-property delivery.timeout.ms=20000 --command-property request.timeout.ms=10000
```

### 8.3 Boîte à outils

| Question | Commande |
|---|---|
| État général | `bilan` |
| État des conteneurs | `docker compose ps` (hôte) |
| Quorum en détail | `kafka-metadata-quorum.sh --bootstrap-server $BS describe --replication` |
| Brokers vus par un client | `kafka-broker-api-versions.sh --bootstrap-server $BS \| grep '^kafka'` |
| Partitions dégradées | `kafka-topics.sh --bootstrap-server $BS --describe --under-min-isr-partitions` |
| Un topic | `kafka-topics.sh --bootstrap-server $BS --describe --topic T` |
| Configuration d'un topic | `kafka-configs.sh --bootstrap-server $BS --describe --entity-type topics --entity-name T` |
| Configuration effective d'un nœud | `kafka-configs.sh --bootstrap-server $BS --describe --entity-type brokers --entity-name N --all \| grep MOT` |
| Comptes et quotas | `kafka-configs.sh --bootstrap-server $BS --describe --entity-type users` (ou `clients`) |
| Droits | `kafka-acls.sh --bootstrap-server $BS --list` |
| Espace disque | `kafka-log-dirs.sh --bootstrap-server $BS --describe --broker-list N \| tail -1` |
| Traces d'un nœud | `docker compose logs kafka-N --since 3m \| grep -E 'WARN\|ERROR' \| tail` (hôte) |
| Traces de sécurité | `docker compose exec kafka-N grep -iE 'authentication\|handshake' /opt/kafka/logs/server.log \| tail` (hôte) |
| Configuration d'un nœud | `config/kafka/kafka-N/server.properties`, et l'original dans `config/kafka/pristine/` (hôte) |
| Métriques | `q 'requête'`, Grafana sur http://localhost:3000, alertes sur http://localhost:9090/alerts |

Exemples de requêtes utiles :

```bash
# [tools]
q 'sum by (instance) (kafka_log_log_size)'
q 'sum by (topic) (rate(kafka_server_brokertopicmetrics_messagesin_total{topic!=""}[1m]))'
q 'sum by (topic) (kafka_log_log_size)'
```

### 8.4 Fin de tour

```bash
# [hôte] par l'injecteur, une fois la correction validée (N = numéro de l'incident)
scripts/incident.sh N repair
```

```bash
# [tools] bilan de fin de tour, à comparer au bilan de référence
bilan
```

`repair` réapplique l'état de référence, même si votre correction est déjà la bonne. Pour le ticket Logistique, il envoie un événement de plus. Pour le ticket Paiements, il redémarre `kafka-3` une seconde fois, ce qui prend environ 15 s.

## 9. Résultats attendus

### 9.1 Préparation

`bilan` montre un leader de quorum, les brokers `kafka-1`, `kafka-2` et `kafka-3`, toutes les valeurs à 0 et `alertes : []`. Juste après le TP7, une alerte `pending` (`KafkaFencedBrokers`, `KafkaUnderMinIsrPartitions`) peut subsister une trentaine de secondes après l'arrêt de `kafka-4` : attendez, puis relancez `bilan`. Si une alerte reste active, signalez-le au formateur avant de commencer.

### 9.2 Ticket Logistique résolu

- `/tmp/incident4/send-shipment.sh` se termine sans aucune ligne `ERROR`, avec le code de retour 0 (`echo $?`).
- La lecture de `shipments` depuis le début affiche l'événement `SHIP-<horodatage>` avec `"carrier":"colissimo"`.
- Les traces des brokers ne montrent plus de nouvel échec d'authentification pour ce client.

### 9.3 Ticket Paiements résolu

- Les 6 envois passent sans aucune ligne `ERROR`.
- `bilan` : les brokers sont annoncés sous les noms `kafka-1`, `kafka-2`, `kafka-3` ; URP, sous min ISR et indisponibles à 0.
- Les alertes de réplication sont levées dans les 1 à 2 minutes qui suivent la correction.

### 9.4 Ticket Stockage résolu

- La configuration de `clickstream` porte une limite de durée et de taille conforme à la politique de l'équipe Data.
- `q 'sum by (instance) (kafka_log_log_size)'` donne moins de 250 Mo par nœud (262 144 000 octets).
- `alertes` ne contient plus `KafkaBrokerLogSizeHigh`, environ 1 minute après la correction.
- Le topic existe toujours, avec ses données récentes.

## 10. Vérification

- Trois fiches remplies, chacune relue et signée par l'injecteur. La cause tient en une phrase, et la validation cite des commandes et leurs résultats.
- `bilan` final identique au bilan de référence de 7.1.
- Côté hôte, `ls config/kafka/kafka-3/` ne montre pas de fichier `server.properties.before-incident5` : le `repair` du ticket Paiements a bien été lancé.

## 11. Questions de réflexion

1. Ticket Paiements : pourquoi certains envois passent-ils alors que d'autres expirent ?
2. Ticket Paiements : `kafka-3` est `healthy` pour Docker et `unfenced` pour le contrôleur. Pourquoi est-il pourtant inutilisable pour les clients ?
3. Ticket Paiements : pourquoi l'alerte `KafkaNodeDown` ne s'est-elle pas déclenchée ?
4. Ticket Logistique : pourquoi le client a-t-il rencontré deux erreurs successives ? Que disaient les traces du broker dans chaque cas ?
5. Ticket Logistique : fallait-il corriger le client ou le broker ? Pourquoi ?
6. Ticket Stockage : pourquoi une limite de durée d'une heure, seule, n'aurait-elle rien libéré ?
7. Ticket Stockage : pourquoi l'espace n'est-il pas libéré à l'instant même de la correction ?
8. Pour chaque ticket, quelle mesure de prévention proposez-vous ?

## 12. Erreurs fréquentes

| Symptôme | Cause | Correction |
|---|---|---|
| Diagnostic trop rapide, cause « devinée » | Lecture de `scripts/incident.sh` ou de ses commandes `status` | Repartir des symptômes ; la fiche doit montrer la démarche |
| Les indices ont disparu | Redémarrage d'un nœud avant d'avoir observé | Observer d'abord ; redémarrer est une correction, pas un diagnostic |
| Paiements : les symptômes reviennent après un redémarrage de `kafka-3` | Le fichier de configuration fautif n'a pas été corrigé | Comparer avec `config/kafka/pristine/`, corriger, puis redémarrer |
| Logistique : code retour 0, et pourtant l'envoi a échoué | `kafka-console-producer.sh` ne signale pas un échec d'envoi par son code retour | Lire les lignes `ERROR`, puis chercher l'événement dans `shipments` |
| Logistique : une nouvelle erreur apparaît après la première correction | Le fichier client contient plusieurs erreurs | C'est un progrès : lire le nouveau message et continuer |
| Logistique : modification du broker pour accepter le client | Mauvaise cible : le listener est conforme à la politique de sécurité | Corriger le fichier du client |
| Stockage : topic supprimé, ou fichiers effacés dans `/var/lib/kafka` | Correction destructrice | Interdit ; on agit par `retention.ms` et `retention.bytes` |
| Stockage : rien n'est libéré après la correction | Limite de durée seule, ou attente trop courte | Ajouter la limite de taille ; la suppression a lieu toutes les 30 s dans le lab |
| `Incident 5 deja injecte (sauvegarde presente).` | `repair` oublié au tour précédent | `scripts/incident.sh 5 repair`, puis nouvelle injection |
| `wget: bad address 'prometheus'` | Lab en mode `core` | `scripts/start.sh extended` |

## 13. Solution formateur

Voir `corrige.md`.

## 14. Variante avancée

1. **Trois incidents de plus.** Les binômes rapides traitent les incidents 1, 2 et 3 avec les mêmes règles. Tickets : « Un broker ne répond plus » (1), « La facturation prend du retard, mais nos consommateurs tournent » (2), « Des partitions sont sous-répliquées alors que tous les conteneurs sont `healthy` » (3).
2. **Runbook.** À partir de votre fiche du ticket Paiements, rédigez l'entrée du runbook d'astreinte : symptômes, indicateurs, commandes, correction, validation, et ce qui change en production.
3. **Prévention.** Pour le ticket Stockage, proposez une politique applicable à tous les topics créés par l'équipe Data, et dites comment l'imposer.
