# Démarrer le lab Kafka Administration sur la VM pilote

Ce guide permet de monter le lab sur une VM Linux, de vérifier qu'il fonctionne et de l'essayer. Comptez environ 30 minutes, dont le téléchargement des images. Les supports sont dans un dépôt git que vous clonez sur la VM.

Le lab est un cluster Apache Kafka 4.3 de trois nœuds, avec son poste d'administration, ses interfaces web et sa supervision. Tout tourne dans des conteneurs Docker sur la seule VM. Rien n'est installé en dehors de Docker.

## 1. Avant de commencer

Sur la VM, vérifiez les points suivants.

| Point | Attendu | Commande |
|---|---|---|
| Système | Linux 64 bits x86_64, par exemple Ubuntu 24.04 | `uname -m` affiche `x86_64` |
| Processeur | 4 cœurs | `nproc` |
| Mémoire | 16 Go conseillés, dont 6 Go disponibles pour Docker | `free -g` |
| Disque | 15 Go libres au minimum | `df -h /` |
| Docker Engine | 29 ou plus | `docker version` |
| Docker Compose | v2 ou plus | `docker compose version` |
| Droits | `docker ps` fonctionne sans `sudo` | `docker ps` |
| Git | Installé, avec accès au dépôt | `git --version` |
| Ports libres | 8080, 8888, 3000, 9090, 8081, 8083, 9092, 9192, 9292, 9392 | `ss -ltn` |

Si `docker ps` répond `permission denied`, ajoutez votre compte au groupe Docker, puis reconnectez-vous :

```bash
sudo usermod -aG docker "$USER"
```

Cette appartenance donne des droits équivalents à root sur la VM. Utilisez une VM dédiée à ce test.

Pour télécharger les images, la VM doit joindre `docker.io` et `ghcr.io` en HTTPS. Si ce n'est pas possible, passez par le lot hors ligne (section 3, variante B).

## 2. Récupérer les supports

```bash
git clone https://github.com/formateurdata/kafka-administration.git
cd kafka-administration/lab
```

Toutes les commandes suivantes se lancent depuis ce dossier `lab/`. Si le dépôt est privé, utilisez les identifiants ou le jeton d'accès que nous vous avons communiqués. Pour mettre à jour les supports plus tard : `git pull`.

## 3. Préparer la VM

Choisissez une seule variante.

**Variante A : la VM accède à Internet.**

```bash
bash scripts/prepare.sh
```

Le script télécharge les images, vérifie qu'elles sont complètes et génère les certificats TLS propres au lab. Aucune image n'est construite sur la VM. Il se termine par `Poste pret`.

**Variante B : pas d'accès aux registres.** Placez dans un même dossier de la VM les trois fichiers `images-amd64.tar.gz`, `images-amd64.tar.gz.sha256` et `images-amd64.txt` que nous vous remettons à part (ils ne sont pas dans le dépôt), puis :

```bash
bash scripts/offline-bundle.sh load /chemin/vers/le-dossier
bash scripts/prepare.sh --no-pull
```

## 4. Démarrer et vérifier

Vous accéderez aux interfaces par un tunnel SSH (section 5). Publiez donc les ports du lab sur la seule interface locale de la VM :

```bash
cp env.template .env
sed -i 's/^LAB_BIND_ADDRESS=.*/LAB_BIND_ADDRESS=127.0.0.1/' .env
```

Puis démarrez :

```bash
bash scripts/start.sh extended
bash scripts/validate.sh
```

Le démarrage prend quelques minutes. `validate.sh` doit se terminer par :

```
[ OK ] Environnement valide.
```

Le mode `extended` est celui de la formation : trois nœuds Kafka, le poste d'administration, Kafka UI, JupyterLab, Prometheus, Grafana et un générateur de trafic. Il utilise environ 4 Go de mémoire. Le mode `full` ajoute Schema Registry et Kafka Connect :

```bash
bash scripts/start.sh full
```

## 5. Ouvrir les interfaces

JupyterLab n'écoute que sur la VM elle-même, à dessein. Les autres interfaces sont aussi atteintes par un tunnel SSH, ce qui évite d'ouvrir des ports sur le réseau. Depuis votre poste, lancez la commande suivante et laissez-la ouverte (remplacez `compte@ADRESSE_VM`) :

```bash
ssh -N \
  -L 8888:127.0.0.1:8888 \
  -L 8080:127.0.0.1:8080 \
  -L 3000:127.0.0.1:3000 \
  -L 9090:127.0.0.1:9090 \
  compte@ADRESSE_VM
```

Ouvrez ensuite dans le navigateur de votre poste :

| Interface | Adresse | Identifiants |
|---|---|---|
| JupyterLab (notebooks des TP) | http://localhost:8888 | aucun |
| Kafka UI | http://localhost:8080 | aucun |
| Grafana (tableau de bord Kafka) | http://localhost:3000 | `admin` / `kafka-lab`, ou consultation anonyme |
| Prometheus | http://localhost:9090 | aucun |

En mode `full`, ajoutez `-L 8081:127.0.0.1:8081 -L 8083:127.0.0.1:8083` pour Schema Registry et Kafka Connect.

Vous devez voir un cluster `ONLINE` à trois brokers dans Kafka UI, et des courbes dans Grafana.

## 6. Essayer quelques commandes

Le poste d'administration est un conteneur qui contient tous les outils `kafka-*.sh` :

```bash
docker compose exec tools bash
```

Dans ce conteneur, `$BS` désigne les trois nœuds du cluster :

```bash
# Lister les topics
kafka-topics.sh --bootstrap-server $BS --list

# État du quorum de contrôle
kafka-metadata-quorum.sh --bootstrap-server $BS describe --status

# Créer un topic répliqué trois fois, puis le décrire
kafka-topics.sh --bootstrap-server $BS --create --topic essai --partitions 3 --replication-factor 3
kafka-topics.sh --bootstrap-server $BS --describe --topic essai

# Écrire puis relire trois messages
printf "un\ndeux\ntrois\n" | kafka-console-producer.sh --bootstrap-server $BS --topic essai
kafka-console-consumer.sh --bootstrap-server $BS --topic essai --from-beginning --max-messages 3 --timeout-ms 15000

# Supprimer le topic
kafka-topics.sh --bootstrap-server $BS --delete --topic essai
```

Attendu : le topic est créé, chaque partition a un leader et trois répliques synchronisées (`Isr` contient les nœuds 1, 2 et 3), et la lecture renvoie `un`, `deux`, `trois` (dans un ordre quelconque, car le topic a trois partitions). Le message `Processed a total of 3 messages` termine la lecture. Quittez avec `exit`.

Dans JupyterLab, ouvrez `tps/`, puis un dossier `jourN`, un dossier `tpM_*` et son fichier `enonce.ipynb`. Les notebooks pilotent le cluster, y compris l'arrêt et le redémarrage de nœuds.

## 7. Test complet (facultatif)

`selftest.sh` rejoue les commandes clés des exercices : panne et redémarrage de brokers, utilisateurs et ACL, quotas, ajout d'un broker, réassignation de partitions, rolling restart et réplication MirrorMaker 2. Il nettoie ce qu'il crée.

```bash
bash scripts/selftest.sh
```

Prévoyez une douzaine de minutes. Il doit se terminer par `Selftest reussi`.

## 8. Arrêter et remettre à zéro

| Action | Commande |
|---|---|
| Arrêter sans perdre les données | `bash scripts/stop.sh` |
| Redémarrer | `bash scripts/start.sh extended` |
| Tout remettre à zéro (topics, ACL, configurations, données) | `bash scripts/reset.sh --yes` |

Faites un `reset.sh --yes` à la fin de vos essais : le lab doit être vierge au début de la formation.

## 9. En cas de problème

| Symptôme | Cause probable | Que faire |
|---|---|---|
| `permission denied` sur `/var/run/docker.sock` | Compte hors du groupe `docker` | Section 1, puis reconnexion |
| `prepare.sh` : `Images indisponibles` | Registres inaccessibles, proxy ou filtrage | Autoriser `docker.io` et `ghcr.io`, ou variante B. Si un proxy est imposé, le déclarer au démon Docker ([documentation](https://docs.docker.com/engine/daemon/proxy/)) |
| `start.sh` : `Certificats absents` | `prepare.sh` non exécuté | Lancer `bash scripts/prepare.sh` |
| `port is already allocated` | Un autre service utilise un port | Libérer le port. Pour 8080, 3000, 9090, 8081, 8083 ou 8888, copier `env.template` en `.env` et changer la valeur. Les ports Kafka (9092, 9192, 9292, 9392) sont fixes |
| Un conteneur redémarre ou est tué | Mémoire insuffisante | Vérifier `free -g` ; passer en mode `core` : `bash scripts/start.sh core` |
| Page web inaccessible depuis votre poste | Tunnel SSH fermé ou redirection refusée | Relancer la commande de la section 5. Le serveur SSH doit autoriser les redirections locales |

Pour un diagnostic :

```bash
docker compose ps
docker compose logs kafka-1 --tail 50
```

## 10. Sécurité

Ce lab est un environnement de formation, pas un modèle de production. Ses réglages sont volontairement ouverts :

- Sans le réglage `LAB_BIND_ADDRESS=127.0.0.1` de la section 4, Kafka UI, Grafana (identifiants fixes), Prometheus et, en mode `full`, Schema Registry (8081) et l'API REST de Kafka Connect (8083) sont publiés sur toutes les interfaces réseau de la VM, sans authentification. Les ports Kafka 9092 à 9392 le sont aussi, et le compte `ANONYMOUS` y est super-utilisateur. Quiconque joint la VM sur ces ports administre le cluster ; l'API de Connect permet en plus de lire et d'écrire des fichiers dans son conteneur. Vérifiez avec `ss -ltn` que ces ports n'écoutent que sur `127.0.0.1`.
- Docker contourne un pare-feu local de type `ufw` pour les ports qu'il publie. Même avec ce réglage, filtrez au niveau réseau (groupe de sécurité, pare-feu de l'infrastructure) et n'autorisez que SSH (port 22) depuis les postes de test.
- JupyterLab a accès au socket Docker de la VM, donc un accès root. Il n'est lié qu'à `127.0.0.1` pour cette raison.
- N'exposez jamais cette VM sur Internet.

## 11. Ce qu'il faut nous renvoyer

- la sortie de `bash scripts/validate.sh`, et celle de `selftest.sh` si vous l'avez lancé ;
- `docker version` et `docker compose version` ;
- en cas d'échec : `docker compose ps` et les journaux du service concerné, avant tout `reset.sh`.

Merci de nous signaler aussi toute restriction réseau rencontrée (registres, proxy, tunnels SSH).
