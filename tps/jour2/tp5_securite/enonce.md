# TP5 : sécuriser l'accès au cluster

| Repère | Valeur |
|---|---|
| Moment | Jour 2, 09:50-10:45, après la démo d'ouverture du TP (09:45-09:50) |
| Compétences | C9 Listeners, TLS et SASL/SCRAM ; C10 ACL et quotas |
| Slides de rappel | S43 (vue d'ensemble), S44 (protocoles des listeners), S45 (TLS), S47 (SCRAM en KRaft), S48 (ACL), S49 (quotas) |
| Solution formateur | `corrige.md` (point 13) |
| Version notebook | `enonce.ipynb` dans JupyterLab (http://localhost:8888), mêmes étapes en Python ; démo d'ouverture : `demos-tp/demo-tp5.ipynb` |

Conventions : `# [hôte]` sur votre machine dans `lab/`, `# [tools]` dans `docker compose exec tools bash`. Les sections 7, 8 et 9 utilisent la même numérotation.

Dans `tools`, `$BS` désigne le listener INTERNAL (sans sécurité, réservé à l'administration dans ce lab) et `$BS_SECURE` le listener SECURE (`kafka-N:19094`, SASL_SSL avec SCRAM-SHA-512).

---

## 1. Objectifs

À la fin du TP, vous savez :

- lire la configuration de sécurité d'un listener : protocole, mécanisme SASL, magasins TLS, authorizer ;
- créer des comptes SCRAM stockés dans les métadonnées KRaft ;
- écrire le fichier de configuration d'un client SASL_SSL ;
- distinguer un échec d'authentification d'un refus d'autorisation, côté client et dans `kafka-authorizer.log` ;
- donner le minimum de droits à un producteur et à un consommateur ;
- limiter le débit d'un client par un quota et mesurer le ralentissement.

## 2. Durée

55 minutes, après 5 minutes de démonstration d'ouverture.

| Étape | Durée |
|---|---|
| 7.1 Lire la configuration du listener SECURE | 6 min |
| 7.2 Créer les comptes SCRAM | 5 min |
| 7.3 Se connecter en administrateur | 4 min |
| 7.4 Autoriser le producteur `alice` | 7 min |
| 7.5 Défi : écrire la configuration de `bob` | 5 min |
| 7.6 Autoriser le consommateur `bob` | 8 min |
| 7.7 Auditer les ACL | 3 min |
| 7.8 Quota de production | 12 min |
| 7.9 Matrice des droits | 5 min |

## 3. Prérequis

- Jour 1 terminé : topic `orders` présent.
- Lab démarré, de préférence en mode `extended` : `scripts/start.sh extended` puis `scripts/validate.sh`.
- Fichiers clients dans `/opt/lab/clients/` (conteneur `tools`) : `admin.properties`, `alice.properties`, `secure-template.properties`.
- Notions : slides S43 à S51.

## 4. Architecture utilisée

```
                         conteneur tools
          +---------------------+------------------------------+
          | $BS (INTERNAL)      | $BS_SECURE (SECURE)          |
          | PLAINTEXT           | SASL_SSL + SCRAM-SHA-512     |
          | principal ANONYMOUS | principal User:<nom du compte>|
          +----------+----------+---------------+--------------+
                     |                          |
                     v                          v
        kafka-N:19092                  kafka-N:19094
        administration du lab          TLS : certificat du broker signé par la CA du lab
                                       SASL : compte SCRAM (stocké dans __cluster_metadata)
                                       Autorisation : StandardAuthorizer + ACL
                                       Quotas : par principal (user) ou client.id
```

| Compte | Mot de passe | Rôle | Droits visés |
|---|---|---|---|
| `admin` | `admin-secret` | Administration | Super-utilisateur |
| `alice` | `alice-secret` | Service Commandes (`client.id=orders-service`) | Écrire dans `orders` |
| `bob` | `bob-secret` | Service Facturation (`client.id=billing-service`) | Lire `orders` avec le groupe `billing-secure` |

**Écart volontaire avec la production** : `User:ANONYMOUS` est super-utilisateur pour que le listener INTERNAL reste ouvert aux TP. En production, tous les listeners sont authentifiés et ANONYMOUS n'a aucun droit.

## 5. Contexte métier

L'audit de sécurité de NovaShop exige que les applications ne se connectent plus sans identité. Chaque service doit avoir son compte, avec les droits strictement nécessaires. Le service Commandes a déjà saturé le cluster lors d'un import massif : il doit être bridé.

## 6. Énoncé

1. Vérifiez comment le listener SECURE est protégé.
2. Créez les comptes `admin`, `alice` et `bob`.
3. Donnez à `alice` le droit d'écrire dans `orders`, et à `bob` celui de lire `orders` avec le groupe `billing-secure` uniquement.
4. Prouvez que chaque refus est bien journalisé.
5. Limitez `alice` à 100 Ko/s en production et mesurez l'effet.

## 7. Étapes

**7.1 Lire la configuration du listener SECURE.** Dans `server.properties`, relevez le protocole du listener SECURE, le mécanisme SASL activé, les magasins TLS, la classe d'autorisation et les super-utilisateurs. Puis essayez de lister les topics sur `$BS_SECURE` sans aucune configuration de sécurité.

**7.2 Créer les comptes SCRAM.** Créez `admin`, `alice` et `bob` en SCRAM-SHA-512 avec 8192 itérations. Affichez les comptes.

**7.3 Se connecter en administrateur.** Lisez `admin.properties` et listez les topics sur `$BS_SECURE`. Recommencez avec une copie d'`alice.properties` contenant un mauvais mot de passe.

**7.4 Autoriser le producteur `alice`.** Faites produire `alice` dans `orders` : observez le refus, et retrouvez-le dans `kafka-authorizer.log`. Ajoutez les ACL nécessaires, puis reproduisez.

**7.5 Défi : écrire la configuration de `bob`.** Copiez `secure-template.properties` vers `/tmp/bob.properties` et remplacez chaque valeur entre `< >`. Le `client.id` de `bob` est `billing-service`.

**7.6 Autoriser le consommateur `bob`.** Faites lire `orders` par `bob` avec le groupe `billing-secure` : observez le refus. Ajoutez les ACL de lecture sur le topic et sur le groupe. Lisez 5 messages. Essayez ensuite avec le groupe `analytics`, puis faites produire `bob` dans `orders`.

**7.7 Auditer les ACL.** Listez les ACL du topic `orders`, puis toutes celles de `bob`.

**7.8 Quota de production.** Créez le topic `quota-test` (3 partitions) et donnez à `alice` le droit d'y écrire. Fixez un quota de production de 100 Ko/s pour `alice`. Mesurez le débit et le temps de throttling avec un test de performance de 8 Mo, puis supprimez le quota et mesurez à nouveau.

**7.9 Matrice des droits.** Complétez la matrice de la section 10 en vous appuyant sur les résultats des étapes précédentes.

## 8. Commandes directement exécutables

### 8.1 Configuration du listener SECURE

```bash
# [hôte]
grep -nE '^(listener.security.protocol.map|listener.name.secure|authorizer|super.users|allow.everyone)' \
  config/kafka/kafka-1/server.properties
```

```bash
# [tools]
echo $BS_SECURE
timeout 20 kafka-topics.sh --bootstrap-server $BS_SECURE --list
```

### 8.2 Comptes SCRAM

```bash
# [tools]
kafka-configs.sh --bootstrap-server $BS --alter --entity-type users --entity-name admin \
  --add-config 'SCRAM-SHA-512=[iterations=8192,password=admin-secret]'
kafka-configs.sh --bootstrap-server $BS --alter --entity-type users --entity-name alice \
  --add-config 'SCRAM-SHA-512=[iterations=8192,password=alice-secret]'
kafka-configs.sh --bootstrap-server $BS --alter --entity-type users --entity-name bob \
  --add-config 'SCRAM-SHA-512=[iterations=8192,password=bob-secret]'
kafka-configs.sh --bootstrap-server $BS --describe --entity-type users
```

### 8.3 Connexion administrateur

```bash
# [tools]
cat /opt/lab/clients/admin.properties
kafka-topics.sh --bootstrap-server $BS_SECURE --command-config /opt/lab/clients/admin.properties --list

sed 's/alice-secret/mauvais/' /opt/lab/clients/alice.properties > /tmp/alice-bad.properties
kafka-topics.sh --bootstrap-server $BS_SECURE --command-config /tmp/alice-bad.properties --list
```

### 8.4 Producteur `alice`

```bash
# [tools]
echo 'C010|{"order_id":"O-5001"}' | kafka-console-producer.sh --bootstrap-server $BS_SECURE \
  --command-config /opt/lab/clients/alice.properties --topic orders \
  --reader-property parse.key=true --reader-property 'key.separator=|'
```

```bash
# [hôte] les refus sont journalisés par le broker qui a traité la requête
for n in 1 2 3; do docker compose exec -T kafka-$n sh -c 'grep -h "User:alice is Denied" /opt/kafka/logs/kafka-authorizer.log*'; done
```

```bash
# [tools]
kafka-acls.sh --bootstrap-server $BS --add --allow-principal User:alice \
  --operation Write --operation Describe --topic orders
echo 'C010|{"order_id":"O-5001"}' | kafka-console-producer.sh --bootstrap-server $BS_SECURE \
  --command-config /opt/lab/clients/alice.properties --topic orders \
  --reader-property parse.key=true --reader-property 'key.separator=|'
```

### 8.5 Défi

```bash
# [tools]
cat /opt/lab/clients/secure-template.properties
cp /opt/lab/clients/secure-template.properties /tmp/bob.properties
# À vous : remplacez les valeurs entre < > dans /tmp/bob.properties (sed ou vi).
```

### 8.6 Consommateur `bob`

```bash
# [tools]
kafka-console-consumer.sh --bootstrap-server $BS_SECURE --command-config /tmp/bob.properties \
  --topic orders --group billing-secure --from-beginning --max-messages 5 --timeout-ms 15000

kafka-acls.sh --bootstrap-server $BS --add --allow-principal User:bob \
  --operation Read --operation Describe --topic orders --group billing-secure

kafka-console-consumer.sh --bootstrap-server $BS_SECURE --command-config /tmp/bob.properties \
  --topic orders --group billing-secure --from-beginning --max-messages 5 --timeout-ms 15000

kafka-console-consumer.sh --bootstrap-server $BS_SECURE --command-config /tmp/bob.properties \
  --topic orders --group analytics --from-beginning --max-messages 5 --timeout-ms 15000

echo 'C011|{"order_id":"O-5002"}' | kafka-console-producer.sh --bootstrap-server $BS_SECURE \
  --command-config /tmp/bob.properties --topic orders \
  --reader-property parse.key=true --reader-property 'key.separator=|'
```

### 8.7 Audit des ACL

```bash
# [tools]
kafka-acls.sh --bootstrap-server $BS --list --topic orders
kafka-acls.sh --bootstrap-server $BS --list --principal User:bob
```

### 8.8 Quota de production

```bash
# [tools]
kafka-topics.sh --bootstrap-server $BS --create --topic quota-test --partitions 3 --replication-factor 3
kafka-topics.sh --bootstrap-server $BS --describe --topic quota-test
kafka-acls.sh --bootstrap-server $BS --add --allow-principal User:alice \
  --operation Write --operation Describe --topic quota-test

kafka-configs.sh --bootstrap-server $BS --alter --entity-type users --entity-name alice \
  --add-config producer_byte_rate=102400
kafka-configs.sh --bootstrap-server $BS --describe --entity-type users --entity-name alice

kafka-producer-perf-test.sh --bootstrap-server $BS_SECURE --command-config /opt/lab/clients/alice.properties \
  --topic quota-test --num-records 8000 --record-size 1000 --throughput -1 --print-metrics \
  | grep -E 'records sent|throttle-time'

kafka-configs.sh --bootstrap-server $BS --alter --entity-type users --entity-name alice \
  --delete-config producer_byte_rate
kafka-producer-perf-test.sh --bootstrap-server $BS_SECURE --command-config /opt/lab/clients/alice.properties \
  --topic quota-test --num-records 8000 --record-size 1000 --throughput -1 | tail -1
```

## 9. Résultats attendus

### 9.1 Listener SECURE

- `listener.security.protocol.map` associe `SECURE` à `SASL_SSL`.
- `listener.name.secure.sasl.enabled.mechanisms=SCRAM-SHA-512`.
- Magasins PKCS12 : `kafka-1.keystore.p12` (identité du broker), `truststore.p12` (CA du lab).
- `listener.name.secure.ssl.client.auth=none` : pas de certificat client exigé.
- `authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer`, `super.users=User:admin;User:ANONYMOUS`, `allow.everyone.if.no.acl.found=false`.
- Sans configuration de sécurité, la commande échoue avec `The AdminClient thread has exited` et une `java.lang.OutOfMemoryError: Java heap space` dans le fil d'erreur.

### 9.2 Comptes

```
SCRAM credential configs for user-principal 'bob' are SCRAM-SHA-512=iterations=8192
SCRAM credential configs for user-principal 'admin' are SCRAM-SHA-512=iterations=8192
SCRAM credential configs for user-principal 'alice' are SCRAM-SHA-512=iterations=8192
```

Les mots de passe ne sont jamais affichés.

### 9.3 Connexion

`admin` liste tous les topics. Le mauvais mot de passe donne :

```
SaslAuthenticationException: Authentication failed during authentication due to invalid credentials with SASL mechanism SCRAM-SHA-512
```

### 9.4 `alice`

- Sans ACL : `{orders=TOPIC_AUTHORIZATION_FAILED}` puis `TopicAuthorizationException: Not authorized to access topics: [orders]`.
- Dans `kafka-authorizer.log` : `Principal = User:alice is Denied operation = DESCRIBE ... on resource = Topic:LITERAL:orders ... based on rule DefaultDeny`.
- Après l'ACL : envoi sans erreur.

### 9.5 Défi

Un fichier sans aucun `<` ni `>`, avec `SASL_SSL`, `SCRAM-SHA-512`, `bob`, `bob-secret` et `billing-service`.

### 9.6 `bob`

| Essai | Résultat |
|---|---|
| Lecture sans ACL | `TopicAuthorizationException: Not authorized to access topics: [orders]` |
| Lecture avec ACL, groupe `billing-secure` | 5 commandes affichées |
| Lecture avec le groupe `analytics` | `GroupAuthorizationException: Not authorized to access group: analytics` |
| Production | `ClusterAuthorizationException: Cluster authorization failed.` |

### 9.7 ACL

Le topic `orders` porte 4 ACL : `WRITE` et `DESCRIBE` pour `alice`, `READ` et `DESCRIBE` pour `bob`. `bob` a 2 ACL sur le topic et 2 sur le groupe `billing-secure`.

### 9.8 Quota

| Mesure | Avec quota | Sans quota |
|---|---|---|
| Débit final | environ 0,3 Mo/s | 8 à 10 Mo/s |
| `produce-throttle-time-avg` | plus de 100 ms | absent ou 0 |

Pendant le test bridé, les lignes intermédiaires alternent des intervalles rapides et des intervalles presque à l'arrêt.

## 10. Vérification

Matrice des droits à compléter (oui ou non, et la preuve) :

| Action | `admin` | `alice` | `bob` |
|---|---|---|---|
| Lister les topics | | | |
| Écrire dans `orders` | | | |
| Lire `orders` avec le groupe `billing-secure` | | | |
| Lire `orders` avec le groupe `analytics` | | | |
| Écrire dans `quota-test` | | | |

```bash
# [tools]
kafka-acls.sh --bootstrap-server $BS --list
kafka-configs.sh --bootstrap-server $BS --describe --entity-type users
```

Contrôle : 3 comptes SCRAM, aucun quota restant pour `alice`, ACL conformes à la matrice.

## 11. Questions de réflexion

1. Pourquoi un client sans configuration de sécurité obtient-il une erreur de mémoire plutôt qu'un message clair ?
2. Où sont stockés les comptes SCRAM en mode KRaft ? Qui peut les lire ?
3. Pourquoi `alice` a-t-elle besoin de `Describe` en plus de `Write` ?
4. Pourquoi `bob` reçoit-il une erreur d'autorisation sur le **cluster** quand il essaie de produire, et pas sur le topic ?
5. Le quota de 100 Ko/s est-il global ou par broker ? Que montre le débit mesuré ?
6. Quels sont les deux écarts de sécurité de ce lab qui seraient inacceptables en production ?
7. Comment renouveler le mot de passe de `alice` sans interruption de service ?

## 12. Erreurs fréquentes

| Symptôme | Cause | Correction |
|---|---|---|
| `OutOfMemoryError: Java heap space`, `The AdminClient thread has exited` | Client en PLAINTEXT sur un listener TLS | `--command-config` avec `security.protocol=SASL_SSL` |
| `SSL handshake failed` côté broker | Idem, vu du broker | Idem |
| `Authentication failed ... invalid credentials` | Mot de passe erroné, ou compte non créé | `kafka-configs.sh --describe --entity-type users` |
| `Client SASL mechanism 'SCRAM-SHA-256' not enabled in the server` | Mécanisme du client différent de celui du listener | `sasl.mechanism=SCRAM-SHA-512` |
| `TopicAuthorizationException` | Aucune ACL pour ce principal sur ce topic | `kafka-acls.sh --add ...` puis `--list` |
| `GroupAuthorizationException` | ACL de groupe absente ou autre nom de groupe | ACL `Read` sur `--group` |
| `--command-config` ignoré, erreur `PLAINTEXT` | Chemin erroné vers le fichier | `cat` du fichier avant usage ; chemin absolu |
| Le quota n'a pas d'effet | Quota posé sur `bob` au lieu d'`alice`, ou sur `--entity-type clients` avec un autre `client.id` | `kafka-configs.sh --describe --entity-type users` |

## 13. Solution formateur

Voir `corrige.md`.

## 14. Variante avancée

1. Donnez à `alice` tous les droits sur les topics dont le nom commence par `orders.`, avec une seule ACL. Faites-lui créer `orders.returns`, puis `payments.refunds`. Quels topics `alice` voit-elle quand elle les liste ?
2. Ajoutez une ACL qui **refuse** la lecture d'`orders` à `bob`, alors qu'une ACL l'autorise déjà. Quelle règle l'emporte ? Retrouvez la règle appliquée dans `kafka-authorizer.log`. Retirez ensuite cette ACL de refus.
3. Passez le quota d'`alice` sur son `client.id` (`orders-service`) au lieu de son principal. Dans quel cas ce choix est-il préférable ?
