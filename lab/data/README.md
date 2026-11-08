# Jeux de donnees NovaShop

Fichiers montes en lecture seule dans le conteneur `tools` (`/opt/lab/data`)
et dans le conteneur `connect` (`/opt/lab/data`).

| Fichier | Format | Utilisation |
|---|---|---|
| `orders.txt` | `cle|valeur` : id client, commande JSON | TP2 (production avec cle), TP3 |
| `product-prices.txt` | `cle|valeur` : id produit, prix | TP3 (topic compacte) |
| `clickstream-source.txt` | une ligne JSON par evenement | Demo D3 (connecteur FileStream source) |

Le separateur de cle est `|` (les valeurs JSON contiennent des `:`).
Production avec cle :

    kafka-console-producer.sh --bootstrap-server $BS --topic orders \
      --reader-property parse.key=true --reader-property key.separator='|' \
      < /opt/lab/data/orders.txt
