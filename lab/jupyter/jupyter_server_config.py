# Configuration de JupyterLab dans le lab (montee dans /etc/jupyter).
import os

c = get_config()  # noqa: F821

# [LAB] aucune authentification par defaut : port publie sur 127.0.0.1 seulement.
# JUPYTER_TOKEN, facultatif, protege le serveur par un jeton. VS Code en a besoin :
# sans jeton, il n'obtient pas de cookie XSRF et la creation du noyau est refusee (403).
c.IdentityProvider.token = os.environ.get("JUPYTER_TOKEN", "")
c.ServerApp.password = ""

# Les participants ne voient que les notebooks : tps/ et demos-tp/, montes
# dans /notebooks par docker-compose.yml. Le reste de la formation reste
# monte au chemin de l'hote, pour hote() et labkafka.
c.ServerApp.root_dir = "/notebooks"

# Sources des notebooks masquees : on ouvre les .ipynb, pas les .source.py.
c.ContentsManager.hide_globs = [
    "__pycache__", "*.pyc", "*.pyo", ".DS_Store", "*~", "*.source.py",
]
