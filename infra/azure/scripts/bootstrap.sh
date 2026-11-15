#!/usr/bin/env bash
# ------------------------------------------------------------------
# Preparation de la VM Azure du lab, lancee une fois par cloud-init (root).
#
# Reprend livrables-client/Demarrage-du-lab-VM-pilote.md, sections 1 a 4 :
# Docker Engine, clone du depot, prepare.sh, ports lies a 127.0.0.1,
# puis start.sh et validate.sh.
#
# Parametres : /etc/kafka-lab/bootstrap.env (ecrit par Terraform).
# Journal    : /var/log/kafka-lab-bootstrap.log
# Resultat   : /var/log/kafka-lab-bootstrap.status (OK ou ECHEC)
# ------------------------------------------------------------------
set -euo pipefail

LOG=/var/log/kafka-lab-bootstrap.log
STATUS=/var/log/kafka-lab-bootstrap.status
exec >>"$LOG" 2>&1

echo "EN COURS" >"$STATUS"
trap 'echo "ECHEC (voir $LOG)" >"$STATUS"' ERR

# shellcheck disable=SC1091
source /etc/kafka-lab/bootstrap.env

echo "== $(date -u +%FT%TZ) Docker Engine (depot officiel Docker)"
export DEBIAN_FRONTEND=noninteractive
install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
chmod a+r /etc/apt/keyrings/docker.asc
# shellcheck disable=SC1091
codename="$(. /etc/os-release && echo "$VERSION_CODENAME")"
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu $codename stable" \
  >/etc/apt/sources.list.d/docker.list
apt-get update -q
apt-get install -y -q docker-ce docker-ce-cli containerd.io docker-compose-plugin
systemctl enable --now docker

# Droits equivalents a root sur la VM : la VM est dediee au lab.
usermod -aG docker "$LAB_USER"

echo "== $(date -u +%FT%TZ) Depot, images et certificats (compte $LAB_USER)"
# runuser -l ouvre une session neuve : l'appartenance au groupe docker est prise en compte.
runuser -l "$LAB_USER" -c "bash -s" <<EOF
set -euo pipefail
if [[ ! -d \$HOME/kafka-administration/.git ]]; then
  git clone '$REPO_URL' "\$HOME/kafka-administration"
fi
cd "\$HOME/kafka-administration"
git checkout '$REPO_REF'
cd lab
bash scripts/prepare.sh
if [[ ! -f .env ]]; then
  cp env.template .env
fi
sed -i 's/^LAB_BIND_ADDRESS=.*/LAB_BIND_ADDRESS=127.0.0.1/' .env
# Jeton de JupyterLab (vide : sans jeton). Lu par docker compose a chaque start.sh.
sed -i 's/^JUPYTER_TOKEN=.*/JUPYTER_TOKEN=${JUPYTER_TOKEN:-}/' .env
if [[ '$START_LAB' == 1 ]]; then
  bash scripts/start.sh '$LAB_MODE'
  bash scripts/validate.sh
fi
EOF

echo "== $(date -u +%FT%TZ) Termine"
echo "OK" >"$STATUS"
