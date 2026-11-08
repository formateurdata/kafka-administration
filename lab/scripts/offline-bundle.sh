#!/usr/bin/env bash
# Images preconstruites de tous les profils, y compris scale (TP7).
# Usage : bash scripts/offline-bundle.sh save [--platform linux/amd64|linux/arm64] [dossier]
#         bash scripts/offline-bundle.sh load <dossier>
# Aucune construction ni copie de certificats ou de volumes Kafka.
set -euo pipefail
source "$(dirname "$0")/lib.sh"

usage() { die "Usage : $0 save [--platform linux/amd64|linux/arm64] [dossier] | load <dossier>"; }
images() { dc $ALL_PROFILES config --images | sort -u; }

save_bundle() {
  local platform="" dir="$LAB_DIR/offline-bundle" img
  while (( $# )); do
    case "$1" in
      --platform) platform="${2:-}"; [[ -n "$platform" ]] || usage; shift 2 ;;
      -*) usage ;;
      *) dir="$1"; shift ;;
    esac
  done
  [[ -n "$platform" ]] || platform="linux/$(docker version --format '{{.Server.Arch}}')"
  case "$platform" in linux/amd64|linux/arm64) ;; *) usage ;; esac
  mkdir -p "$dir"
  local arch="${platform#linux/}" list out
  list="$(images)"
  info "Preparation des images pour $platform"
  while IFS= read -r img; do
    info "$img ($platform)"
    # Les tags locaux sont produits par build-images.sh, jamais construits ici.
    if [[ "$img" == kafka-lab/* ]]; then
      docker image inspect --platform "$platform" "$img" >/dev/null \
        || die "Variante locale absente : $img ($platform). Lancer build-images.sh sur le poste formateur."
    else
      docker pull --quiet --platform "$platform" "$img" >/dev/null
    fi
  done <<<"$list"
  out="$dir/images-$arch.tar.gz"
  info "Sauvegarde dans $out"
  # shellcheck disable=SC2046
  docker save --platform "$platform" $(tr '\n' ' ' <<<"$list") | gzip -1 > "$out.part"
  mv "$out.part" "$out"
  sha256_of "$out" > "$out.sha256"
  printf '%s\n' "$list" > "$dir/images-$arch.txt"
  ok "Lot $platform sauvegarde. Livrer avec l'archive des supports correspondante."
}

load_bundle() {
  (( $# == 1 )) || usage
  local dir="$1" arch archive img expected
  arch="$(docker version --format '{{.Server.Arch}}')"
  archive="$dir/images-$arch.tar.gz"
  [[ -f "$archive" && -f "$archive.sha256" && -f "$dir/images-$arch.txt" ]] \
    || die "Lot incomplet pour linux/$arch dans $dir."
  expected="$(cat "$archive.sha256")"
  [[ "$(sha256_of "$archive")" == "$expected" ]] || die "Empreinte du lot incorrecte."
  # Refuser un lot d'une autre livraison avant de charger plusieurs Go.
  diff -u "$dir/images-$arch.txt" <(images) || die "Les images du lot ne correspondent pas aux supports."
  info "Chargement hors ligne de $archive"
  docker load -i "$archive"
  while IFS= read -r img; do
    docker image inspect --platform "linux/$arch" "$img" >/dev/null \
      || die "Image ou architecture absente apres chargement : $img"
  done < <(images)
  ok "Images chargees. Etape suivante : bash scripts/prepare.sh --no-pull"
}

case "${1:-}" in
  save) shift; save_bundle "$@" ;;
  load) shift; load_bundle "$@" ;;
  *) usage ;;
esac
