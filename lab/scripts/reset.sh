#!/usr/bin/env bash
# ------------------------------------------------------------------
# Reinitialisation COMPLETE du lab : retour a l'etat d'origine.
#
#   - supprime conteneurs, reseau et volumes (topics, offsets, ACL,
#     utilisateurs SCRAM, metriques Prometheus, reglages Grafana,
#     cluster cloud du TP9) ;
#   - restaure les server.properties et mm2.properties d'origine
#     (config/kafka/pristine/) apres sauvegarde des versions modifiees ;
#   - conserve les certificats TLS, l'agent JMX et les images.
#
# Usage : scripts/reset.sh [--yes]
# ------------------------------------------------------------------
set -euo pipefail
source "$(dirname "$0")/lib.sh"

if [[ "${1:-}" != "--yes" ]]; then
  warn "Toutes les donnees du lab seront supprimees (topics, messages, ACL, metriques)."
  read -r -p "Confirmer la reinitialisation ? [o/N] " answer
  [[ "$answer" =~ ^[oOyY]$ ]] || { info "Abandon."; exit 0; }
fi

info "Suppression des conteneurs et des volumes"
# shellcheck disable=SC2086
dc $ALL_PROFILES down -v --remove-orphans

info "Restauration des configurations des noeuds"
backup_dir="$LAB_DIR/config/kafka/backup-$(date +%Y%m%d-%H%M%S)"
restored=0
for n in 1 2 3 4; do
  current="$LAB_DIR/config/kafka/kafka-$n/server.properties"
  pristine="$LAB_DIR/config/kafka/pristine/kafka-$n.server.properties"
  if ! cmp -s "$current" "$pristine"; then
    mkdir -p "$backup_dir"
    cp "$current" "$backup_dir/kafka-$n.server.properties"
    cp "$pristine" "$current"
    restored=$((restored + 1))
  fi
done
if (( restored > 0 )); then
  ok "$restored fichier(s) restaure(s) ; versions modifiees sauvegardees dans ${backup_dir#"$LAB_DIR"/}"
else
  ok "Configurations deja d'origine"
fi

# Configuration de MirrorMaker 2 (TP9), modifiable pendant le TP.
mm2="$LAB_DIR/config/mirrormaker/mm2.properties"
mm2_pristine="$LAB_DIR/config/kafka/pristine/mm2.properties"
if [[ -f "$mm2_pristine" ]] && ! cmp -s "$mm2" "$mm2_pristine"; then
  mkdir -p "$backup_dir"
  cp "$mm2" "$backup_dir/mm2.properties"
  cp "$mm2_pristine" "$mm2"
  ok "mm2.properties restaure ; version modifiee sauvegardee dans ${backup_dir#"$LAB_DIR"/}"
fi

# Sauvegarde laissee par un incident 5 injecte mais pas repare (scripts/incident.sh) :
# perimee apres la restauration, elle empecherait une nouvelle injection.
for incident_backup in "$LAB_DIR"/config/kafka/kafka-*/server.properties.before-incident*; do
  [[ -e "$incident_backup" ]] || continue
  mkdir -p "$backup_dir"
  mv "$incident_backup" "$backup_dir/"
  ok "Sauvegarde d'incident ${incident_backup#"$LAB_DIR"/} retiree ; copie dans ${backup_dir#"$LAB_DIR"/}"
done

# Fichier log4j2 : seule la copie d'origine est restauree si elle existe.
if [[ -f "$LAB_DIR/config/kafka/pristine/log4j2.yaml" ]] && \
   ! cmp -s "$LAB_DIR/config/kafka/common/log4j2.yaml" "$LAB_DIR/config/kafka/pristine/log4j2.yaml"; then
  cp "$LAB_DIR/config/kafka/pristine/log4j2.yaml" "$LAB_DIR/config/kafka/common/log4j2.yaml"
  ok "log4j2.yaml restaure"
fi

# Regle d'alerte ecrite au TP6 (defi) : retiree pour revenir aux seules regles
# d'origine. Elle est sauvegardee avant suppression.
custom_rule="$LAB_DIR/monitoring/prometheus/rules/tp6-custom.yml"
if [[ -f "$custom_rule" ]]; then
  mkdir -p "$backup_dir"
  mv "$custom_rule" "$backup_dir/tp6-custom.yml"
  ok "Regle tp6-custom.yml retiree ; copie dans ${backup_dir#"$LAB_DIR"/}"
fi

ok "Lab reinitialise. Redemarrage : scripts/start.sh <core|extended|full>"
