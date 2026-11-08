# Fiche de diagnostic d'incident

Une fiche par incident. Remplissez-la **pendant** le diagnostic, pas après : la chronologie est la partie la plus utile pour l'équipe qui lira la fiche. Cette fiche sert aussi de modèle au compte rendu du TP final.

| Champ | Contenu |
|---|---|
| Ticket | |
| Diagnostiqué par | |
| Heure de début | |

## 1. Qualifier le symptôme

- Qui est touché : un client, un topic, un groupe, tout le cluster ?
- Depuis quand ? Qu'est-ce qui a changé juste avant ?
- Message d'erreur exact, recopié tel quel :

```

```

## 2. Observer, du général au particulier

| Niveau | Commande ou source | Constat |
|---|---|---|
| Quorum | | |
| Brokers | | |
| Réplication (URP, sous min ISR, indisponibles) | | |
| Topic ou groupe concerné | | |
| Client | | |
| Traces des nœuds | | |
| Alertes et métriques | | |

## 3. Hypothèses

| Hypothèse | Commande de vérification | Résultat | Retenue ? |
|---|---|---|---|
| | | | |
| | | | |

## 4. Cause

Une phrase : quoi, où, depuis quand.

## 5. Correction

| Heure | Action exacte (commande ou fichier modifié) | Effet observé |
|---|---|---|
| | | |

## 6. Validation

| Preuve | Commande | Résultat |
|---|---|---|
| Le symptôme a disparu | | |
| Les indicateurs sont revenus à la normale | | |
| Les alertes sont levées | | |

## 7. Prévention

Ce qui aurait évité l'incident, ou l'aurait détecté plus tôt : revue de configuration, test automatique, alerte, quota, politique de topic, procédure.

## 8. Chronologie

| Heure | Événement |
|---|---|
| | Début du symptôme |
| | Première observation |
| | Cause identifiée |
| | Correction appliquée |
| | Retour à la normale validé |
