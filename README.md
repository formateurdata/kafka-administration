# Kafka Administration en 2 jours (PLB IKAF)

Supports complets pour animer la formation PLB « Kafka Administration » (référence IKAF), condensée de 3 jours à 2 jours de 7 heures, avec environ 70 % de pratique. Tout est en français et repose sur Apache Kafka 4.3.2 en mode KRaft.

- Programme d'origine : https://www.plb.fr/formation/kafka-administration (mis à jour le 15/12/2025), recopié dans [plan/programme-IKAF.md](plan/programme-IKAF.md).
- Public : administrateurs, ingénieurs d'exploitation, architectes. Niveau intermédiaire.

## Livrables

| | Livrable | Emplacement |
|---|---|---|
| A | Analyse du programme : correspondance 3 jours vers 2 jours, arbitrages, part de pratique | [plan/A-analyse-programme.md](plan/A-analyse-programme.md) |
| B | Déroulé minuté des 2 jours, compétences C1 à C17, points de contrôle, plans de repli | [plan/B-deroule.md](plan/B-deroule.md) |
| C | 73 slides (S1 à S73), au format objectif, contenu, mots-clés, schéma, exemple, commandes, notes orales, transition. Decks PowerPoint sur le modèle PLB, avec le speech du formateur dans les notes et en Markdown | [slides/](slides/), [slides/pptx/README.md](slides/pptx/README.md) |
| D | Lab Docker Compose KRaft : architecture, versions, scripts, tests | [lab/](lab/), [lab/ARCHITECTURE.md](lab/ARCHITECTURE.md), [lab/TESTS.md](lab/TESTS.md) |
| E | Énoncés des 8 TP stagiaires, au format en 14 points, en Markdown, et en notebook Jupyter avec zones `# TODO` en Python | `tps/jourN/tpM_*/enonce.md`, `enonce.ipynb` |
| F | Corrigés des TP (Markdown et notebook exécuté), démonstrations D1 à D7, démonstrations d'ouverture des 8 TP, guide d'animation, dépannage, runbook des 6 incidents | `tps/jourN/tpM_*/corrige.md`, `corrige.ipynb`, [demos/](demos/), [demos-tp/](demos-tp/) et [formateur/demos-ouverture-tp.md](formateur/demos-ouverture-tp.md), [formateur/](formateur/) |
| G | Quiz de positionnement, quiz intermédiaires, quiz final, TP final noté et barème | [evaluation/](evaluation/) |

## Les deux journées

| Jour 1 : comprendre, installer, fiabiliser | Jour 2 : sécuriser, observer, exploiter, diagnostiquer |
|---|---|
| M1 Architecture et KRaft | M5 Sécurité, TP5 SCRAM, SASL_SSL, ACL, quotas |
| M2 Installation, démos D1 et D2, TP1 cluster KRaft, TP2 topics | M6 Observabilité, démo D5, TP6 traces, Prometheus, Grafana, alertes, lag |
| M3 Clients, démo D6, TP3 groupes, offsets, rétention, compaction | M7 Exploitation, démo D7, TP7 ajout de broker, réassignation, log dir, rolling restart, décommissionnement |
| Démo D3 Schema Registry et Connect | TP8 atelier incidents en binômes |
| M4 Réplication, TP4 fiabilité et panne, démo D4 perte de quorum | TP final noté, quiz final, bilan |

Le détail minute par minute est dans le livrable B.

## Démarrage rapide (formateur)

La livraison utilise deux images préconstruites, JupyterLab et Kafka avec l'agent
JMX. Construction, publication amd64/arm64 et archives client/hors ligne :
[lab/images/README.md](lab/images/README.md). Les images sont distribuées sur Docker Hub, sous le compte `m0uaad`.
`prepare.sh` les télécharge ; le poste participant ne construit aucune image.

Prérequis : Docker Desktop récent avec Compose v2, 6 Go alloués à Docker pour le mode conseillé, accès Internet la veille.

```bash
cd lab
bash scripts/prepare.sh     # images préconstruites et certificats TLS locaux
scripts/start.sh extended   # cluster, outils, supervision, trafic continu
scripts/validate.sh         # doit afficher "Environnement valide."
docker compose exec tools bash
```

Les notebooks des TP s'ouvrent dans JupyterLab, sur http://localhost:8888 : voir [lab/jupyter/README.md](lab/jupyter/README.md).

Avant la session, tester tout le lab : `scripts/selftest.sh` (8 min environ sur le poste de test). Préparation détaillée, salle sans Internet et dépannage : [lab/README.md](lab/README.md) et [formateur/depannage.md](formateur/depannage.md).

La personnalisation passe par un fichier `.env` créé à partir de `lab/env.template`. Il est facultatif : toutes les variables ont une valeur par défaut dans `docker-compose.yml`.

## Versions

| Composant | Version |
|---|---|
| Apache Kafka (brokers, contrôleurs, outils, Connect) | 4.3.2, base `apache/kafka:4.3.2`, image du lab `m0uaad/kafka-lab-kafka:4.3.2-1` |
| Confluent Schema Registry | 8.3.2 |
| Kafka UI (kafbat) | 1.5.0 |
| Prometheus JMX exporter (agent Java) | 1.7.0 |
| Prometheus | 3.15.1 |
| Grafana | 13.2.4 |
| kafka-exporter (lag des groupes) | 1.9.0 |

Kafka 4.4.0 est sortie le 22/10/2026. La formation reste sur 4.3.2, dernier correctif de la ligne précédente. La slide S69 et la démo D7 traitent la montée de version.

## Ce qui a été testé

Toutes les commandes des slides, des TP, des démonstrations et du TP final ont été exécutées sur le lab, et les sorties reproduites sont réelles. Détail, dates et mesures : [lab/TESTS.md](lab/TESTS.md).

| Testé | Non testé |
|---|---|
| macOS sur Apple Silicon (`arm64`), Docker Desktop 29.9.2, Compose 5.6.1 | Windows avec WSL2 |
| Modes `core`, `extended`, `full` ; `selftest.sh` : 109 vérifications, 0 échec (dont MirrorMaker 2, profil `mirror`) | Linux `x86_64` |
| Les 6 incidents du TP8, injectés puis réparés | Chargement du lot hors ligne `amd64` sur un PC `x86_64` |
| TP final : 1/13 avant correction, 13/13 après la solution de référence | Authentification mutuelle TLS (mTLS), présentée comme piste |
| | MirrorMaker 2 vers un vrai cluster AWS MSK (authentification IAM) |
| Les 32 requêtes du tableau de bord Grafana, provisionnement vérifié par l'API | Affichage visuel du tableau de bord dans un navigateur |

## Arborescence

```
plan/         programme PLB, livrables A et B
slides/       jour1/ (M0 à M4), jour2/ (M5 à M8) : fiches en 8 champs
              Kafka-Administration-J1.pptx, -J2.pptx, speech-J1.md, speech-J2.md : générés
              pptx/ : générateur et sources YAML ; template/ : modèle PLB
lab/          livrable D : docker-compose.yml, config/, monitoring/, security/, scripts/, data/
tps/          jour1/ (TP1 à TP4), jour2/ (TP5 à TP8) : enonce.md et corrige.md,
              enonce.ipynb et corrige.ipynb générés depuis notebook.source.py
demos/        démonstrations formateur D1 à D7
demos-tp/     démonstrations d'ouverture des TP (notebooks de 5 min)
formateur/    guide d'animation, démos d'ouverture des TP, visite des interfaces, dépannage, runbook des incidents
evaluation/   quiz et TP final
livrables-client/  Kafka-Administration-visite-des-interfaces.pdf : visite guidée des interfaces, généré
              visite-interfaces/ : générateur, texte (contenu.yaml) et captures
AGENTS.md     conventions et décisions, pour maintenir ces supports
```
