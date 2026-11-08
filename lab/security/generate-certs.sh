#!/usr/bin/env bash
# ------------------------------------------------------------------
# Generation des certificats TLS du lab (CA privee + certificats noeuds).
#
# Usage normal : lance par scripts/prepare.sh dans un conteneur Kafka
# (keytool y est disponible), sans dependance sur la machine hote.
# Usage direct possible si keytool (Java 17+) est installe :
#     cd lab/security && ./generate-certs.sh
#
# Produit dans security/certs/ :
#   ca.p12, ca.crt                   autorite de certification du lab
#   truststore.p12                   contient ca.crt (pour brokers et clients)
#   kafka-N.keystore.p12 (N=1..4)    cle + certificat du noeud, signe par la CA
#   kafka-cloud.keystore.p12         noeud du cluster cible de MirrorMaker 2 (TP9)
#   client-alice.keystore.p12        certificat client (variante mTLS du TP5)
#
# Chaque certificat de noeud porte un SAN (Subject Alternative Name)
# avec son nom d'hote Docker : la verification du nom d'hote cote client
# (ssl.endpoint.identification.algorithm=https, valeur par defaut) reste active.
#
# [LAB] mot de passe unique et CA auto-signee. En production : PKI
# d'entreprise, un mot de passe par magasin, rotation des certificats.
# ------------------------------------------------------------------
set -euo pipefail

PASS="${TLS_STORE_PASSWORD:-novashop-lab}"
VALIDITY=825
OUT="$(cd "$(dirname "$0")" && pwd)/certs"
NODES="${NODES:-kafka-1 kafka-2 kafka-3 kafka-4 kafka-cloud}"

# Sans FORCE=1, la CA existante est conservee et seuls les certificats
# manquants sont crees (ajout d'un noeud a un lab deja prepare).
if [[ "${FORCE:-0}" == "1" ]]; then
  rm -rf "$OUT"
fi
mkdir -p "$OUT"
cd "$OUT"

if [[ -f ca.p12 && -f truststore.p12 ]]; then
  echo "[certs] 1/4 et 2/4 CA et truststore deja presents : conserves"
else
  rm -f ./*.p12 ./*.crt
  echo "[certs] 1/4 Autorite de certification du lab"
  keytool -genkeypair -noprompt -alias ca -keyalg RSA -keysize 2048 \
    -dname "CN=NovaShop Lab CA, O=NovaShop Formation, C=FR" \
    -ext bc:c -validity "$VALIDITY" \
    -keystore ca.p12 -storetype PKCS12 -storepass "$PASS" -keypass "$PASS"
  keytool -exportcert -rfc -alias ca -keystore ca.p12 -storepass "$PASS" -file ca.crt

  echo "[certs] 2/4 Truststore commun"
  keytool -importcert -noprompt -alias ca -file ca.crt \
    -keystore truststore.p12 -storetype PKCS12 -storepass "$PASS"
fi

sign() {
  # $1 = alias / nom de fichier, $2 = DN, $3 = extension SAN (vide si aucune)
  local name="$1" dname="$2" san="$3"
  local ks="$name.keystore.p12"
  local san_opt=()
  if [[ -f "$ks" ]]; then
    echo "[certs]     $name : deja present"
    return 0
  fi
  [[ -n "$san" ]] && san_opt=(-ext "SAN=$san")

  keytool -genkeypair -noprompt -alias "$name" -keyalg RSA -keysize 2048 \
    -dname "$dname" -validity "$VALIDITY" "${san_opt[@]}" \
    -keystore "$ks" -storetype PKCS12 -storepass "$PASS" -keypass "$PASS"
  keytool -certreq -alias "$name" -keystore "$ks" -storepass "$PASS" -file "$name.csr"
  keytool -gencert -rfc -alias ca -keystore ca.p12 -storepass "$PASS" \
    -infile "$name.csr" -outfile "$name.crt" -validity "$VALIDITY" "${san_opt[@]}" \
    -ext "KU=digitalSignature,keyEncipherment" -ext "EKU=serverAuth,clientAuth"
  # La chaine complete (CA puis certificat signe) remplace le certificat auto-signe.
  keytool -importcert -noprompt -alias ca -file ca.crt -keystore "$ks" -storepass "$PASS"
  keytool -importcert -noprompt -alias "$name" -file "$name.crt" -keystore "$ks" -storepass "$PASS"
  rm -f "$name.csr"
}

echo "[certs] 3/4 Certificats des noeuds : $NODES"
for node in $NODES; do
  sign "$node" "CN=$node, OU=Kafka, O=NovaShop Formation, C=FR" "dns:$node,dns:localhost"
done

echo "[certs] 4/4 Certificat client pour la variante mTLS (alice)"
sign client-alice "CN=alice, OU=Clients, O=NovaShop Formation, C=FR" ""

# Lisibles par l'utilisateur du conteneur Kafka (appuser, uid 1000).
chmod 644 ./*.p12 ./*.crt
echo "[certs] Termine :"
ls -1 "$OUT"
