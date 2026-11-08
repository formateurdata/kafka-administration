# Kafka Administration : supports participants

Livraison @@VERSION@@. Docker avec Compose v2, bash et navigateur requis.
Allouer 6 Go à Docker pour extended, 8 Go pour full. Windows : utiliser WSL2,
avec les supports dans le système de fichiers Linux.

Les images sont préconstruites. Si leurs références dans livraison.json commencent
par kafka-lab/, elles sont locales : utiliser le lot hors ligne ci-dessous.
Pour des références publiées dans un registre, depuis le dossier lab/ :

```bash
bash scripts/prepare.sh
bash scripts/start.sh extended
bash scripts/validate.sh
```

Hors ligne, charger le lot correspondant à ce poste avant la préparation :

```bash
bash scripts/offline-bundle.sh load /chemin/vers/le-lot
bash scripts/prepare.sh --no-pull
bash scripts/start.sh extended
bash scripts/validate.sh
```

Ouvrir http://localhost:8888 pour les TP, http://localhost:8080 pour Kafka UI,
http://localhost:3000 pour Grafana. Les modes core, extended, full et le profil
scale restent disponibles. Les configurations des TP se modifient dans lab/config/.
Pour remettre le lab à zéro : bash scripts/reset.sh --yes (efface les données).

Les certificats sont générés sur chaque poste. Les images ne réduisent pas
la mémoire nécessaire au lab. Aucun outil de compilation n'est requis.

Précautions de lab : JupyterLab sans jeton sur 127.0.0.1 et socket Docker monté
(accès administrateur au moteur). Grafana : admin / kafka-lab, lecture anonyme.
Nœuds Kafka combinés, User:ANONYMOUS super-utilisateur, délai de rééquilibrage
nul, contrôle de rétention à 30 secondes, alerte disque à 250 Mo : réglages
pédagogiques à ne pas reprendre en production. Utiliser une machine de TP.

Les empreintes des fichiers et références des images sont dans livraison.json.
