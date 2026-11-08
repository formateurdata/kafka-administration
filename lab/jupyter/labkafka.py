"""Outils communs aux notebooks des TP et des demos (lab Kafka Administration).

Dans un notebook, une seule ligne suffit :

    from labkafka import *

Le module expose :
  - les adresses du lab : BS, BS_SECURE, BS_CLOUD, PROMETHEUS, KAFKA_UI, LAB_DIR ;
  - tools(cmd) : commande dans le conteneur tools, comme "# [tools]" des enonces ;
  - hote(cmd)  : commande dans le dossier lab/, comme "# [hote]" des enonces
                 (docker compose, fichiers de configuration) ;
  - admin()    : un AdminClient confluent-kafka neuf, connecte au listener INTERNAL ;
  - verifier() : controle d'une cellule de verification ;
  - attendre() : attente d'une condition, avec delai maximal ;
  - tableau()  : affichage d'une liste de lignes sous forme de tableau.
"""
import os
import subprocess
import time
from collections import deque

from confluent_kafka.admin import AdminClient

BS = os.environ.get("BS", "kafka-1:19092,kafka-2:19092,kafka-3:19092")
BS_SECURE = os.environ.get("BS_SECURE", "kafka-1:19094,kafka-2:19094,kafka-3:19094")
# Cluster cloud du TP9 (profil mirror) : listener INTERNAL, sans authentification.
BS_CLOUD = os.environ.get("BS_CLOUD", "kafka-cloud:19092")
FORMATION_DIR = os.environ.get("FORMATION_DIR", "/formation")
LAB_DIR = os.path.join(FORMATION_DIR, "lab")
PROMETHEUS = "http://prometheus:9090"
KAFKA_UI = "http://kafka-ui:8080"
TOOLS = "kafka-tools"

__all__ = ["BS", "BS_SECURE", "BS_CLOUD", "FORMATION_DIR", "LAB_DIR", "PROMETHEUS", "KAFKA_UI",
           "Sortie", "ARevoir", "tools", "hote", "admin", "verifier", "attendre", "tableau"]


class Sortie(str):
    """Texte produit par une commande, avec son code retour dans .code."""

    code = 0
    affichee = False

    def _ipython_display_(self):
        # En fin de cellule, Jupyter afficherait le texte une seconde fois.
        if not self.affichee:
            print(self)


class ARevoir(AssertionError):
    """Levee par verifier() quand le resultat attendu n'est pas obtenu."""


def _executer(args, timeout, afficher, cwd=None):
    try:
        r = subprocess.run(args, capture_output=True, text=True, timeout=timeout, cwd=cwd)
        texte, code = r.stdout + r.stderr, r.returncode
    except subprocess.TimeoutExpired as e:
        # Sortie partielle : toujours en octets, meme avec text=True.
        texte = "".join(f.decode(errors="replace") if isinstance(f, bytes) else f
                        for f in (e.stdout, e.stderr) if f)
        texte += "\n[delai de %d s depasse : commande interrompue]" % timeout
        code = 124
    sortie = Sortie(texte.rstrip("\n"))
    sortie.code = code
    if afficher and sortie:
        print(sortie)
        sortie.affichee = True
    return sortie


def tools(commande, timeout=120, afficher=True):
    """Execute une commande bash dans le conteneur tools ($BS, $BS_SECURE, kafka-*.sh)."""
    return _executer(["docker", "exec", TOOLS, "bash", "-c", commande], timeout, afficher)


def hote(commande, timeout=120, afficher=True):
    """Execute une commande bash dans le dossier lab/ (docker compose, fichiers du lab)."""
    if FORMATION_DIR == "/formation":
        print("[attention] lab demarre sans scripts/start.sh : les commandes docker compose "
              "qui creent un conteneur peuvent echouer.")
    return _executer(["bash", "-c", commande], timeout, afficher, cwd=LAB_DIR)


# Derniers clients crees : un AdminClient detruit avant la reponse fait echouer la
# requete (_DESTROY), comme dans admin().describe_cluster().result().
_CLIENTS = deque(maxlen=32)


def admin(**config):
    """AdminClient neuf sur le listener INTERNAL ; config complete ou remplace les reglages.

    Un client neuf a chaque appel (4 ms environ) : un client qui a deja servi garde le
    coordinateur de chaque groupe en cache. Quand ce coordinateur change de broker
    (reequilibrage des leaders 5 minutes apres le redemarrage d'un noeud), confluent-kafka
    2.16 renvoie alors NOT_COORDINATOR a describe_consumer_groups pendant plus d'une
    minute, et list_consumer_group_offsets arrete le noyau (erreur de segmentation).
    """
    # log_level 2 : sans le journal de librdkafka, qui s'afficherait dans le notebook.
    # Un broker arrete ferait ecrire a chaque client neuf une erreur de connexion (niveau 3) ;
    # les erreurs des requetes arrivent de toute facon par les futurs (.result()).
    client = AdminClient({"bootstrap.servers": BS, "log_level": 2, **config})
    _CLIENTS.append(client)
    return client


def verifier(condition, message):
    """Affiche OK si la condition est vraie, sinon leve ARevoir avec le message."""
    if not condition:
        raise ARevoir("A revoir : " + message)
    print("OK  " + message)


def attendre(condition, timeout=60, pas=2, message="condition"):
    """Rappelle condition() jusqu'a obtenir une valeur vraie, et la renvoie."""
    debut = time.time()
    while True:
        try:
            valeur = condition()
        except Exception:
            valeur = None
        if valeur:
            return valeur
        if time.time() - debut >= timeout:
            raise TimeoutError("%s : non atteinte en %d s" % (message, timeout))
        time.sleep(pas)


def tableau(lignes, entetes):
    """Affiche des lignes (listes ou tuples) en tableau Markdown dans le notebook."""
    from IPython.display import Markdown, display

    def cellule(v):
        return str(v).replace("|", "\\|")

    md = ["| " + " | ".join(entetes) + " |", "|" + "---|" * len(entetes)]
    md += ["| " + " | ".join(cellule(v) for v in ligne) + " |" for ligne in lignes]
    display(Markdown("\n".join(md)))
