# Lab « Kafka Administration » : cluster NovaShop

Environnement de travaux pratiques de la formation PLB IKAF condensée en 2 jours.
Il fournit un cluster Apache Kafka 4.3.2 en mode KRaft, avec l'outillage d'administration, de supervision et l'écosystème, sur un seul poste avec Docker.

- Architecture, choix techniques et écarts avec la production : [ARCHITECTURE.md](ARCHITECTURE.md)
- Ce qui a été testé, et sur quelle machine : [TESTS.md](TESTS.md)

## 1. Contenu

| Service | Rôle | Accès depuis l'hôte | Profil |
|---|---|---|---|
| `kafka-1`, `kafka-2`, `kafka-3` | Nœuds combinés broker + contrôleur KRaft | `localhost:9092`, `9192`, `9292` | Core |
| `tools` (conteneur `kafka-tools`) | Poste d'administration : toutes les commandes `kafka-*.sh` | `docker compose exec tools bash` | Core |
| `kafka-ui` | Interface graphique (kafbat/kafka-ui) | http://localhost:8080 | Core |
| `jupyter` | JupyterLab des TP et des démos : client Python `confluent-kafka`, CLI Docker (image préconstruite) | http://localhost:8888 (sans mot de passe, depuis le poste seulement) | Core |
| `prometheus` | Collecte des métriques et alertes | http://localhost:9090 | `monitoring` |
| `grafana` | Tableau de bord « Kafka - Cluster NovaShop (lab) » | http://localhost:3000 (admin / kafka-lab, ou consultation anonyme) | `monitoring` |
| `kafka-exporter` | Lag des consumer groups | interne (`kafka-exporter:9308`) | `monitoring` |
| `loadgen-producer`, `loadgen-consumer` | Trafic continu sur le topic `loadgen` (100 messages/s) | aucun | `load` |
| `schema-registry` | Confluent Schema Registry 8.3.2 | http://localhost:8081 | `ecosystem` |
| `connect` | Kafka Connect distribué (connecteurs FileStream) | http://localhost:8083 | `ecosystem` |
| `kafka-4` | Broker seul, ajouté au TP7 | `localhost:9392` | `scale` |
| `kafka-cloud` | Second cluster d'un nœud, cible de MirrorMaker 2 (joue AWS MSK) | interne (`kafka-cloud:19092`, `$BS_CLOUD` ; SASL_SSL sur `kafka-cloud:9096`) | `mirror` |
| `mirrormaker` | MirrorMaker 2 en mode dédié, flux `onprem->cloud` (TP9) | aucun | `mirror` |

Les profils sont regroupés en trois modes :

| Mode | Profils | Utilisation |
|---|---|---|
| `core` | aucun | TP1 à TP5, poste peu puissant |
| `extended` | `monitoring`, `load` | TP6 à TP8, TP final (mode conseillé pour toute la formation) |
| `full` | `monitoring`, `load`, `ecosystem` | Démonstration D3 (Schema Registry, Connect) |

Les profils `scale` (TP7) et `mirror` (TP9) se démarrent à part, en plus du mode en cours. Le profil `mirror` ajoute environ 1 Go.

## 2. Prérequis

### Matériel

Mesures réelles sur le poste de test (voir [TESTS.md](TESTS.md)), après une minute de fonctionnement :

| Mode | Mémoire utilisée | Mémoire à allouer à Docker | Poste conseillé |
|---|---|---|---|
| `core` | 1,5 Go | 4 Go | 8 Go de RAM, 4 cœurs |
| `extended` | 4,1 Go | 6 Go | 16 Go de RAM, 4 cœurs |
| `full` | 4,9 Go | 8 Go | 16 Go de RAM, 4 cœurs |

- Chaque nœud Kafka a 512 Mo de heap et une limite de 1 Go. Sa mémoire de processus mesurée est d'environ 700 Mo sous charge.
- Disque : prévoir 10 Go libres. Les images occupent environ 5 Go une fois décompressées pour 1,6 Go téléchargés. Les volumes de données restent sous 300 Mo par nœud pendant la formation, sauf pendant l'incident 6 du TP8 : temporairement 300 Mo par nœud sur un lab vierge, jusqu'à 500 Mo sur un lab chargé par les TP précédents (0,9 à 1,5 Go au total).
- Processeur : la charge reste faible (moins d'un cœur au total en mode `extended`). Les machines virtuelles très lentes allongent surtout les démarrages.

### Logiciels

| Logiciel | Version testée | Remarque |
|---|---|---|
| Docker Desktop  | Docker Engine 29.9.2, Compose 5.6.1 | Compose v2 obligatoire (commande `docker compose`) |
| Shell | bash ou zsh | Les scripts sont en bash |
| Navigateur | récent | Kafka UI, Grafana, Prometheus |

Linux x86_64 a été testé le 09/11/2026 sur une VM Ubuntu 24.04 (voir `TESTS.md`) ; Windows avec WSL2 n'a pas été testé. Les images utilisées existent pour `linux/amd64` et `linux/arm64`.
Sous Windows, lancer les scripts depuis un terminal WSL2, avec le dossier du lab dans le système de fichiers Linux (`~/...`) et non sous `/mnt/c` : les montages depuis `/mnt/c` sont lents et posent des problèmes de droits.

### Réseau

- Accès Internet nécessaire une seule fois, pour `scripts/prepare.sh` : images Docker préconstruites depuis les registres.
- Sans Internet en salle : préparer un lot hors ligne (section 4).
- Ports utilisés sur l'hôte : 9092, 9192, 9292, 9392, 8080, 8888, 9090, 3000, 8081, 8083. Tous sauf les quatre premiers se changent dans `.env` (voir `env.template`). Ils sont publiés sur toutes les interfaces, sauf JupyterLab (`127.0.0.1`) ; sur une VM atteinte par tunnel SSH, `LAB_BIND_ADDRESS=127.0.0.1` dans `.env` les limite tous à la machine elle-même.

## 3. Installation (la veille de la formation)

Utiliser l'archive de supports et les images de la même livraison. Les deux images du lab sont distribuées sur Docker Hub sous le compte `m0uaad` :
voir [images/README.md](images/README.md). `prepare.sh` les télécharge ;
`prepare.sh --no-pull` utilise les images déjà chargées.
Aucune construction n'a lieu sur le poste participant.

```bash
cd lab
bash scripts/prepare.sh     # images préconstruites, vérifications, certificats TLS locaux
scripts/start.sh extended   # démarrage
scripts/validate.sh         # vérification : doit se terminer par "Environnement valide."
```

Facultatif, pour personnaliser ports, mémoire ou versions :

```bash
cp env.template .env        # puis modifier .env ; Docker Compose le lit automatiquement
```

Test complet du lab avant la session (environ 7 minutes) :

```bash
scripts/reset.sh --yes && scripts/start.sh full
scripts/validate.sh && scripts/selftest.sh
scripts/reset.sh --yes      # repartir d'un lab vierge pour les stagiaires
```

`selftest.sh` rejoue les commandes clés des TP1 à TP7 et du TP9 : panne de broker, SCRAM et ACL, quotas, métriques, ajout de broker, réassignation, log dir, rolling restart, décommissionnement et réplication MirrorMaker 2. Il nettoie ce qu'il crée.

## 4. Salle sans Internet : lot hors ligne

Sur le poste formateur connecté, avec les références des images de la livraison :

```bash
bash scripts/offline-bundle.sh save --platform linux/amd64 /chemin/cle-usb   # PC Windows ou Linux x86_64
bash scripts/offline-bundle.sh save --platform linux/arm64 /chemin/cle-usb   # Mac Apple Silicon
```

Sur chaque poste stagiaire :

```bash
bash scripts/offline-bundle.sh load /chemin/cle-usb
bash scripts/prepare.sh --no-pull
```

Chaque lot contient les images d’une architecture, une empreinte SHA-256 et la liste des références. Sa taille dépend de la livraison. Remettre aussi l’archive des supports correspondante ([images/README.md](images/README.md)). Le chargement vérifie l’empreinte et refuse les références différentes. Aucun certificat ni volume de données n’est livré.

## 5. Utilisation quotidienne

| Action | Commande |
|---|---|
| Démarrer | `scripts/start.sh core` (ou `extended`, `full`) |
| Vérifier | `scripts/validate.sh` |
| Ouvrir le shell d'administration | `docker compose exec tools bash` |
| Ouvrir les notebooks des TP | http://localhost:8888 (après `scripts/start.sh`) |
| Arrêter sans perdre les données | `scripts/stop.sh` |
| Supprimer les conteneurs, garder les données | `scripts/stop.sh --down` |
| Tout remettre à zéro (topics, ACL, configurations) | `scripts/reset.sh` |
| Démarrer `kafka-4` (TP7) | `docker compose --profile scale up -d kafka-4` |
| Démarrer le cluster cloud puis MirrorMaker 2 (TP9) | `docker compose --profile mirror up -d kafka-cloud`, puis `docker compose --profile mirror up -d mirrormaker` |
| Injecter un incident (TP8, formateur) | `scripts/incident.sh list` puis `scripts/incident.sh <n> inject` |
| TP final (formateur) | `scripts/final/prepare-final.sh`, puis `scripts/final/check-final.sh` |

Dans le conteneur `tools` :

- `$BS` vaut `kafka-1:19092,kafka-2:19092,kafka-3:19092` (listener INTERNAL, PLAINTEXT) ;
- `$BS_SECURE` vaut `kafka-1:19094,kafka-2:19094,kafka-3:19094` (listener SECURE, SASL_SSL) ;
- `/opt/lab/clients/` contient les fichiers de propriétés clients, `/opt/lab/data/` les jeux de données NovaShop, `/opt/lab/kafka-config/` les `server.properties` (lecture seule).

```bash
docker compose exec tools bash
kafka-topics.sh --bootstrap-server $BS --list
kafka-metadata-quorum.sh --bootstrap-server $BS describe --status
```

Les notebooks des TP (`tps/jourN/tpM_*/enonce.ipynb`) et des démos (`demos-tp/`) s'ouvrent dans JupyterLab, qui ne montre que ces deux dossiers. Le conteneur `jupyter` monte en plus toute la formation au même chemin que l'hôte, et pilote le lab par le socket Docker : `docker compose` y fonctionne comme dans un terminal ouvert dans `lab/`. Fonctionnement et génération des notebooks : [jupyter/README.md](jupyter/README.md).

| Réglage de JupyterLab | Lab | Production |
|---|---|---|
| Authentification | Aucune par défaut (ni jeton, ni mot de passe). Jeton fixe si `JUPYTER_TOKEN` est défini, nécessaire pour VS Code ([jupyter/README.md](jupyter/README.md)) | Jamais sans authentification : JupyterHub ou authentification d'entreprise |
| Socket Docker monté | Oui, pour arrêter et redémarrer des nœuds depuis un notebook. Il donne un accès root au moteur Docker du poste | Jamais |
| Port publié | `127.0.0.1:8888` seulement, injoignable depuis le réseau de la salle | Sans objet |

Les fichiers `config/kafka/kafka-N/server.properties` se modifient sur l'hôte, puis le nœud se redémarre avec `docker compose restart kafka-N`. `scripts/reset.sh` restaure les versions d'origine (`config/kafka/pristine/`) après en avoir fait une sauvegarde.

## 6. Arborescence

```
lab/
  docker-compose.yml          services, profils, volumes, healthchecks
  jupyter/                    image JupyterLab, labkafka.py (ancien module des notebooks, plus importé),
                              build_notebooks.py (génération et exécution des notebooks)
  env.template                modèle de fichier .env (toutes les valeurs ont un défaut)
  config/
    kafka/common/             start-node.sh (formatage + démarrage), log4j2.yaml
    kafka/kafka-1..4/         server.properties de chaque nœud (modifiables)
    kafka/kafka-cloud/        server.properties et start-cloud.sh du cluster cloud (TP9)
    mirrormaker/              mm2.properties de MirrorMaker 2 (TP9)
    kafka/pristine/           copies d'origine utilisées par reset.sh
    connect/                  connect-distributed.properties
  security/
    generate-certs.sh         CA de lab, keystores PKCS12 (keytool)
    certs/                    généré par prepare.sh (non versionné)
  clients/                    plaintext, admin, alice, modèle SASL_SSL
  monitoring/
    jmx-exporter/             règles broker et client (agent intégré à l’image)
    prometheus/               prometheus.yml, rules/kafka-alerts.yml (9 alertes)
    grafana/                  provisioning + dashboards/kafka-cluster.json
  data/                       orders.txt, product-prices.txt, clickstream-source.txt
  scripts/
    prepare.sh start.sh stop.sh reset.sh validate.sh selftest.sh
    incident.sh offline-bundle.sh lib.sh
    loadgen/                  producteur et consommateur de charge
    final/                    prepare-final.sh, check-final.sh (TP final)
```

## 7. Dépannage rapide

| Symptôme | Cause probable | Action |
|---|---|---|
| `start.sh` refuse de démarrer : « Port 8080 (kafka-ui) deja utilise par … », ou `docker compose up` échoue avec « port is already allocated » | Port pris par une autre application ou par un conteneur d'un autre projet ; `start.sh` le nomme | Arrêter ce qui occupe le port, ou changer le port dans `.env` (8080, 8888, 9090, 3000, 8081, 8083). Les ports 9092 à 9392 sont fixes. |
| Un nœud Kafka redémarre en boucle ou reste `unhealthy` | Mémoire Docker insuffisante, `server.properties` invalide | `docker compose logs kafka-N --tail 50` ; corriger ou `scripts/reset.sh` |
| `validate.sh` : « Prometheus : 3 noeuds Kafka collectés (up=0) » | Image incompatible ou règles JMX incorrectes | Vérifier les références de livraison, `scripts/prepare.sh --no-pull`, puis les journaux Kafka |
| Commandes CLI lentes et nombreuses lignes `WARN` | Un nœud est arrêté : le client essaie encore son adresse | Normal pendant les TP de panne. Filtrer avec `2>/dev/null` si besoin. |
| Erreurs `UnknownHostException` dans les traces | Nom d'hôte erroné dans `advertised.listeners` | Voir TP8, incident 5 |
| Grafana vide après un `reset` | Prometheus vient de redémarrer | Attendre 1 minute (intervalle de collecte 15 s, fenêtres de calcul 1 min) |
| JupyterLab répond dans le navigateur mais VS Code ne s'y connecte pas (`404 /login`, `403 _xsrf` dans `docker compose logs jupyter`) | Serveur sans jeton : `/login` n'existe pas et VS Code ne peut pas créer le noyau | Redémarrer avec le jeton fixe : `cd lab && JUPYTER_TOKEN=kafka-lab docker compose up -d jupyter`. Dans VS Code, utiliser `http://localhost:8888/?token=kafka-lab` comme serveur Jupyter existant. Le noyau doit tourner dans le conteneur `jupyter`, pas sur l'hôte. |


