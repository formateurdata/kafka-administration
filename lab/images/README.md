# Images préconstruites et livraison du lab

Le poste participant charge les images et génère ses certificats. Il ne construit
aucune image et n'installe aucun paquet Python. Docker Compose conserve les modes
`core`, `extended`, `full` et le profil `scale` du TP7.

| Image du lab | Base et contenu | Référence Docker Hub |
|---|---|---|
| JupyterLab | Python 3.13.15, JupyterLab 4.6.4, confluent-kafka 2.16.0, requests 2.34.2, CLI Docker 29.9.0 | `m0uaad/kafka-lab-jupyter:1` |
| Kafka commune | `apache/kafka:4.3.2`, agent JMX 1.7.0, démarrage KRaft et scripts de charge | `m0uaad/kafka-lab-kafka:4.3.2-1` |

Kafka UI, Schema Registry, Prometheus, Grafana et kafka-exporter gardent leurs
images officielles et leurs versions actuelles. La base Apache reste inchangée.
Les configurations Kafka, Connect, JMX, Prometheus et Grafana restent montées depuis
le poste. Les contextes Docker utilisent une liste fermée : certificats, clés
privées, fichiers `.env`, supports et données Kafka n'entrent pas dans les images.

Les images sont accessibles depuis les pages [JupyterLab](https://hub.docker.com/r/m0uaad/kafka-lab-jupyter)
et [Kafka](https://hub.docker.com/r/m0uaad/kafka-lab-kafka). Les participants lancent
`bash scripts/prepare.sh` depuis `lab/`, ou chargent les lots hors ligne.

## Construire sur le poste formateur

Depuis la racine du dépôt, avec Internet et Docker Buildx :

```bash
bash lab/scripts/build-images.sh --platform linux/amd64,linux/arm64
```

Le script construit JupyterLab, puis Kafka, et charge les deux architectures dans
le magasin local. Ce chargement exige le magasin containerd de Docker Desktop
récent ou Docker Engine 29. Le téléchargement de l'agent JMX et son empreinte
SHA-256 sont vérifiés sur le poste formateur, puis dans le Dockerfile Kafka.

## Publier dans le registre choisi

La publication utilise le compte Docker Hub `m0uaad`, connecté avec `docker login`.
Pour publier une nouvelle version, exporter ses références avant construction,
constitution de lot ou vérification :

```bash
export KAFKA_IMAGE=m0uaad/kafka-lab-kafka:4.3.2-1
export JUPYTER_IMAGE=m0uaad/kafka-lab-jupyter:1
bash lab/scripts/build-images.sh --push --platform linux/amd64,linux/arm64
```

Les manifestes publiés sont affichés par `imagetools inspect` : vérifier la présence
de `linux/amd64` et `linux/arm64`. Ne pas réutiliser un tag après modification du
contenu : incrémenter la version du lab. Les références avec digest sont également
acceptées pour les archives de supports. Le script `--push` attend des tags de
registre, et ne change ni les droits de visibilité ni l'authentification du registre.
Avec un ancien fichier `.env`, remplacer manuellement la référence
`apache/kafka:4.3.2` par celle de l'image commune ; elle ne contient pas les scripts
ni l'agent JMX nécessaires à cette livraison. L'agent ne modifie pas ce fichier.

Le registre doit permettre la lecture depuis les postes clients, ou les images
être remises sous forme de lot hors ligne.

Références Docker : [construction multiplateforme](https://docs.docker.com/build/building/multi-platform/)
et [gestion des images dans Compose](https://docs.docker.com/reference/compose-file/services/#pull_policy).

## Si le réseau de Docker Desktop bloque l'envoi

Un export OCI conserve les mêmes variantes et les mêmes couches. Il permet de
séparer la construction locale de l'envoi depuis macOS ou Linux, avec
[regctl](https://regclient.org/install/) sur le poste formateur uniquement :

```bash
export REGCTL_CONFIG=/tmp/kafka-regctl-config.json
regctl registry set docker.io --cred-helper docker-credential-desktop \
  --blob-max 10485760 --blob-chunk 10485760

docker buildx build --platform linux/amd64,linux/arm64 \
  --output type=oci,dest=/tmp/jupyter.oci.tar \
  --tag m0uaad/kafka-lab-jupyter:1 lab/jupyter
regctl image import m0uaad/kafka-lab-jupyter:1 /tmp/jupyter.oci.tar

docker buildx build --platform linux/amd64,linux/arm64 \
  --output type=oci,dest=/tmp/kafka.oci.tar \
  --tag m0uaad/kafka-lab-kafka:4.3.2-1 -f lab/images/Dockerfile lab
regctl image import m0uaad/kafka-lab-kafka:4.3.2-1 /tmp/kafka.oci.tar
```

Adapter le gestionnaire d'identifiants si Docker Desktop n'est pas utilisé.
Cette configuration temporaire contient la référence au gestionnaire ; les
identifiants restent dans celui-ci. Vérifier ensuite les manifestes et l'accès
anonyme avant de remettre les supports. Les participants n'ont pas besoin de
regctl. Incrémenter les tags pour toute modification du contenu des images.

## Constituer les supports participants

```bash
python3 lab/scripts/release-client.py \
  --version 2026.11.04-2 \
  --kafka-image "$KAFKA_IMAGE" \
  --jupyter-image "$JUPYTER_IMAGE" \
  --output dist/Kafka-Administration-2026.11.04-2.tar.gz
```

Pour une livraison locale sans registre, construire avec des références
`kafka-lab/kafka:4.3.2-1` et `kafka-lab/jupyter:1`, puis les fournir aussi au générateur. L'archive contient les énoncés Markdown et notebooks, les démonstrations
d'ouverture, le PDF client disponible, les configurations et les scripts clients.
Elle exclut corrigés, sources de notebooks, notes du formateur, évaluations,
certificats, sauvegardes et données d'exécution. Les configurations Kafka sont
reprises des copies d'origine, même si un incident est en cours sur le poste source.
`livraison.json` donne les références d'images et l'empreinte de chaque fichier ;
un fichier `.sha256` accompagne l'archive. Le générateur refuse d'écraser une archive.

## Constituer les lots hors ligne

Avec les mêmes références que pour les supports :

```bash
bash lab/scripts/offline-bundle.sh save --platform linux/amd64 dist/hors-ligne-2026.11.04-2
bash lab/scripts/offline-bundle.sh save --platform linux/arm64 dist/hors-ligne-2026.11.04-2
```

Le script télécharge les images publiées pour la plateforme cible. Pour les tags
locaux `kafka-lab/*`, il exige que la variante ait déjà été construite. Il ne
construit rien. Chaque lot comprend `images-ARCH.tar.gz`, son empreinte `.sha256`
et la liste `images-ARCH.txt`. `docker save --platform` exige un moteur récent
(API 1.48 ou ultérieure) et le magasin containerd. Ne pas mélanger les lots de deux
livraisons : le chargement compare les références avec celles des supports.

Sur un poste client, extraire les supports puis, dans leur dossier `lab/` :

```bash
bash scripts/offline-bundle.sh load /chemin/vers/hors-ligne
bash scripts/prepare.sh --no-pull
bash scripts/start.sh extended
bash scripts/validate.sh
```

L'empreinte est vérifiée avant chargement. `prepare.sh --no-pull` vérifie les images
et utilise des conteneurs sans réseau pour vérifier les dépendances et générer les
certificats. `start.sh` impose `--no-build --pull never`. Aucun JAR séparé à livrer.

## Vérifier avant remise au client

```bash
python3 -m unittest discover -s lab/tests -v
cd lab
bash scripts/prepare.sh --no-pull
bash scripts/reset.sh --yes
bash scripts/start.sh full
bash scripts/validate.sh
bash scripts/selftest.sh
bash scripts/reset.sh --yes
```

Extraire ensuite l'archive dans un dossier neuf et rejouer chargement, préparation,
démarrage et validation. Vérifier que les certificats sont créés sur ce poste et
que les configurations se modifient toujours depuis un TP. Tester aussi sur un
vrai poste `amd64` : une construction sous émulation ne valide pas ce parcours.
Noter les résultats réels et les limites dans [TESTS.md](../TESTS.md).

Les besoins en RAM restent de 6 Go alloués à Docker pour `extended`, 8 Go pour
`full`. JupyterLab sans jeton, socket Docker, mots de passe fixes, nœuds combinés
et réglages accélérés restent des conventions de lab, détaillées dans
[README.md](../README.md) et [ARCHITECTURE.md](../ARCHITECTURE.md).
