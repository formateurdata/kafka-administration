# Notebooks des TP et des démos

Les TP se font dans JupyterLab, servi par le conteneur `jupyter` du lab sur http://localhost:8888. Chaque TP a deux notebooks, générés depuis une source unique :

| Fichier | Public | Contenu |
|---|---|---|
| `tps/jourN/tpM_*/enonce.ipynb` | Participants | Les 14 points du format de TP. Pour chaque étape : une explication, un exemple en ligne de commande, une zone `# TODO` à compléter en Python, une cellule de vérification |
| `tps/jourN/tpM_*/corrige.ipynb` | Formateur | Les mêmes cellules, avec les solutions commentées, les points à faire dire, les réponses aux questions. Il a été exécuté sur le lab : ses sorties sont réelles, avec leur date en tête |
| `tps/jourN/tpM_*/notebook.source.py` | Mainteneur | La source des deux notebooks |
| `demos-tp/demo-tpM.ipynb` | Formateur, à l'écran | La démonstration de 5 minutes qui ouvre chaque TP (texte à dire : [formateur/demos-ouverture-tp.md](../../formateur/demos-ouverture-tp.md)) |

Les `enonce.md` et `corrige.md` restent la version en ligne de commande seule, pour les participants qui préfèrent un terminal. Une modification d'un TP se reporte dans les deux versions.

Les `.ipynb` sont **générés** : ne jamais les modifier à la main, la génération suivante écraserait la modification.

## Environnement

| Élément | Valeur |
|---|---|
| Image | `m0uaad/kafka-lab-jupyter:1`, préconstruite par le formateur depuis `Dockerfile`, chargée par `scripts/prepare.sh` (Python 3.13.15, JupyterLab 4.6.4, confluent-kafka 2.16.0, requests 2.34.2, CLI Docker 29.9.0) |
| Dossiers visibles dans JupyterLab | `tps/` et `demos-tp/` seulement, montés dans `/notebooks` ; les sources `*.source.py` sont masquées (`jupyter_server_config.py`) |
| Formation complète | Montée au même chemin que sur l'hôte (`FORMATION_DIR`, exporté par `scripts/lib.sh`), pour `hote()`, `labkafka.py` et `build_notebooks.py` |
| Kafka | `BS` (INTERNAL) et `BS_SECURE` (SECURE), mêmes valeurs que dans `tools` |
| Docker | Socket monté : `docker compose` fonctionne depuis un notebook comme dans un terminal ouvert dans `lab/` |
| Module commun | `labkafka.py`, importable par `from labkafka import *` pour un usage libre ; aucun notebook de TP ou de démo ne l'importe (voir plus bas) |

`labkafka.py` fournit :

| Nom | Rôle |
|---|---|
| `tools("cmd")` | Commande bash dans le conteneur `tools` : équivalent de `# [tools]` dans les énoncés Markdown |
| `hote("cmd")` | Commande bash dans le dossier `lab/` : équivalent de `# [hôte]` |
| `admin()` | `AdminClient` neuf sur le listener INTERNAL, créé à chaque appel : un client qui a déjà servi garde en cache le coordinateur des groupes, et échoue quand ce coordinateur change de broker (`NOT_COORDINATOR`, voire arrêt du noyau). Les 32 derniers clients restent en mémoire : un client détruit avant la réponse fait échouer la requête avec `_DESTROY` |
| `verifier(condition, message)` | Affiche `OK`, ou lève `ARevoir` avec le message |
| `attendre(fonction, timeout)` | Rappelle la fonction jusqu'à obtenir une valeur vraie |
| `tableau(lignes, entetes)` | Affiche un tableau dans le notebook |
| `BS`, `BS_SECURE`, `BS_CLOUD`, `LAB_DIR`, `PROMETHEUS`, `KAFKA_UI` | Adresses du lab vues depuis le conteneur (`BS_CLOUD` : cluster cloud du TP9) |

`tools()` et `hote()` renvoient la sortie de la commande (texte), avec le code retour dans `.code`. Par défaut ils l'affichent aussi ; `afficher=False` la garde silencieuse pour la traiter en Python.

**Démonstrations et TP : sans `labkafka`.** Les notebooks montrent le travail tel qu'on le fait en entreprise, sans module caché. Pour le terminal : une ligne `!commande` ou une cellule `%%bash` (`!docker exec kafka-tools kafka-topics.sh --bootstrap-server $BS ...` pour les outils Kafka, `!docker compose ...` pour le lab). `output = !command` range les lignes de la sortie dans une liste (`output.n` les joint en un texte) ; dans une ligne `!`, `{expression}` insère une valeur Python et les accolades littérales se doublent (`{{{{.Name}}}}` donne `{{.Name}}` à Docker). Pour Python : `confluent-kafka` directement (`AdminClient`, `Producer`, `Consumer`). Chaque TP commence par une cellule de connexion visible (`BS`, `CONF`, `LAB_DIR`, `os.chdir(LAB_DIR)` : les cellules `!docker compose` et les chemins `config/...` partent du dossier `lab/`) et définit lui-même ses deux fonctions d'aide `wait_for()` et `table()`. Les vérifications sont des `assert` suivis d'un `print("OK ...")`. Un client `AdminClient(CONF)` est créé dans chaque cellule qui en a besoin : un client qui a déjà servi garde en cache le coordinateur des groupes et échoue quand il change de broker. Aucune zone `# TODO` dans les démonstrations : elles montrent le code et l'expliquent. `build_notebooks.py` ne compile pas les cellules qui contiennent `!` ou `%` (syntaxe IPython). `labkafka.py` n'est plus importé par aucun notebook.

Le client Python ne couvre pas toute l'administration. `confluent-kafka` 2.16 n'a pas d'API pour le quorum KRaft, les fonctionnalités (`kafka-features.sh`), les log dirs, la réassignation de partitions, le désenregistrement d'un broker, les quotas, les niveaux de trace des brokers (`broker-loggers`) ni la configuration par défaut de tous les brokers (`--entity-default`). Pour ces sujets, les notebooks passent par la CLI, et le Python sert à exploiter sa sortie (JSON de `kafka-log-dirs.sh` et des plans de réassignation au TP7).

## Particularités de certains TP

| TP | Particularité |
|---|---|
| TP4, TP7, TP8 | Des cellules arrêtent ou redémarrent des nœuds : elles durent plusieurs minutes et ne doivent pas être interrompues |
| TP6 | Mode `extended` obligatoire (Prometheus, Grafana, générateur de charge) |
| TP7 | Crée `kafka-4` (profil `scale`) par `hote()` : le lab doit avoir été démarré par `scripts/start.sh`. Modifie `config/kafka/kafka-1/server.properties` (copie `.bak` à côté) ; `scripts/reset.sh` restaure l'original |
| TP9 | Démarre `kafka-cloud` et `mirrormaker` (profil `mirror`) par `hote()`. Modifie `config/mirrormaker/mm2.properties` à l'étape 9.7 ; `scripts/reset.sh` restaure l'original et supprime le cluster cloud. Corrigé exécuté après les corrigés des TP1 à TP6, comme le lab d'un participant au début du TP9 |
| TP8 | Dans l'énoncé, l'injecteur lance `scripts/incident.sh` dans un terminal du poste de son partenaire. Le corrigé joue lui-même l'injecteur par `hote()` : il donne les causes, à ne pas ouvrir avant la fin de l'atelier |

## Ouvrir les notebooks dans VS Code

Réservé au formateur : VS Code montre tout le dépôt, corrigés et `formateur/` compris. Les participants restent sur JupyterLab, qui ne montre que `tps/` et `demos-tp/`.

Le noyau doit tourner dans le conteneur `jupyter` : un noyau Python de l'hôte ne trouve ni `labkafka`, ni les noms `kafka-N`. VS Code se connecte donc au serveur du lab, qui doit avoir un jeton. Sans jeton, `/login` répond 404, l'extension Jupyter n'obtient pas de cookie XSRF, et la création du noyau est refusée (403).

```bash
# Depuis lab/, avec le mode en cours. Sur un lab démarré, seul le conteneur jupyter est recréé :
# Kafka continue de tourner, les noyaux ouverts sont perdus.
JUPYTER_TOKEN=kafka-lab bash scripts/start.sh extended
```

Puis, dans VS Code, sur le notebook ouvert : *Select Kernel* > *Existing Jupyter Server...* > URL `http://localhost:8888/?token=kafka-lab` > *Python 3 (ipykernel)*.

| Point d'attention | Conséquence |
|---|---|
| JupyterLab avec jeton | Il demande le jeton à la première ouverture ; `http://localhost:8888/lab?token=kafka-lab` l'évite |
| Démarrage suivant | `start.sh` sans la variable retire le jeton. Pour le garder : `JUPYTER_TOKEN=kafka-lab` dans `lab/.env` |
| Corrigés et démos | Enregistrer après avoir exécuté une cellule remplace sa sortie réelle, et seul `--executer` sur un lab vierge la restaure. Tout enregistrement écrit aussi le noyau choisi dans les métadonnées. Travailler dans `enonce.ipynb` |
| Même notebook ouvert dans JupyterLab et VS Code | À éviter : chacun écrase les enregistrements de l'autre |
| Dossier courant du noyau | `/notebooks`, la racine du serveur, et non le dossier du notebook. Les notebooks n'emploient que des chemins absolus (`LAB_DIR`, `/tmp`) |

Le jeton `kafka-lab` est une convention de lab, écrite en clair sur un port publié sur `127.0.0.1` seulement. En production, un jeton est généré et gardé secret.

Essais du 30/10 : les requêtes de l'extension (version 2025.9.1) ont été rejouées sur le lab, mais le parcours dans l'interface de VS Code n'a pas été joué ([TESTS.md](../TESTS.md)).

## Format des sources

Une source est un fichier Python au format « percent » : chaque cellule commence par une ligne `# %%`.

| Marqueur | Cellule |
|---|---|
| `# %%` | Code, dans tous les notebooks |
| `# %% [markdown]` | Texte, dans tous les notebooks ; chaque ligne commence par `# ` |
| `# %% enonce` | Code de l'énoncé seulement : la zone `# TODO` |
| `# %% corrige` | Code du corrigé seulement : la solution |
| `# %% [markdown] enonce` ou `corrige` | Texte réservé à l'un des deux notebooks |

Dans le texte, `@@DATE_EXECUTION@@` est remplacé par la date d'exécution du corrigé ou de la démo.

Conventions :

- Texte en français avec accents ; code et commentaires de code sans accents, comme dans les scripts du lab.
- Une zone `# TODO` initialise toutes les variables qu'elle demande (`None`, `[]`, `{}`). Ainsi l'énoncé s'exécute d'un bout à l'autre et seules les vérifications échouent, avec `ARevoir`.
- Une démonstration d'échec (refus d'ACL, broker arrêté) attrape l'exception et l'affiche : le corrigé doit s'exécuter sans erreur.
- Chaque cellule de vérification commence par `# Vérification 6.x`.

## Générer et exécuter

Depuis la racine de la formation :

```bash
# Générer énoncés, corrigés et démos (sur l'hôte, sans le lab)
uv run --with nbformat lab/jupyter/build_notebooks.py            # tout
uv run --with nbformat lab/jupyter/build_notebooks.py tp1 demo-tp1

# Générer puis exécuter corrigés et démos sur le lab : sorties réelles
docker exec -w "$PWD" jupyter python lab/jupyter/build_notebooks.py --executer tp1 demo-tp1

# Exécuter l'énoncé avec ses TODO vides, sans rien réécrire
docker exec -w "$PWD" jupyter python lab/jupyter/build_notebooks.py --tester-enonce tp1

# Reporter une correction de texte en gardant les sorties enregistrées (sur l'hôte, sans le lab)
uv run --with nbformat lab/jupyter/build_notebooks.py --textes tp2
```

L'exécution modifie le lab comme le ferait un participant : elle se fait sur un lab vierge (`scripts/reset.sh --yes`, puis `scripts/start.sh extended`), TP après TP, dans l'ordre de la formation.

**Attention** : une génération sans `--executer` ni `--textes`, sur l'hôte ou dans le conteneur, réécrit corrigés et démos sans sorties. `--tester-enonce` seul ne réécrit aucun fichier.

`--textes` sert quand seul le texte a changé, par exemple une valeur citée à aligner sur les sorties. Il régénère les notebooks et reprend les sorties et la date d'exécution des corrigés et démos déjà exécutés. Si une cellule de code a changé, même d'un caractère, il refuse de réécrire ce notebook (ligne `ATTENTION`) : la cible doit alors être réexécutée, sur un lab amené à l'état de début du TP (`--rejouer` des TP précédents).

Après une exécution, vérifier :

1. aucune ligne `ATTENTION` en fin de génération : marqueur inconnu, ligne de texte sans `# `, cellule de code qui ne compile pas ;
2. aucune cellule en erreur dans le corrigé (le script s'arrête sur la première) ;
3. avec `--tester-enonce`, l'énoncé n'échoue que sur ses cellules de vérification. Le lab doit être dans l'état attendu au début du TP ; les exemples en ligne de commande de l'énoncé s'exécutent pour de bon ;
4. les valeurs citées dans le texte du corrigé correspondent aux sorties obtenues.
