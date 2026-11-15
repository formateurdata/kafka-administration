# Lab Kafka Administration sur une VM Azure

Ce dossier provisionne avec Terraform une VM Azure dédiée à un participant et y installe le lab tel qu'il tourne en local : mêmes conteneurs, même `docker-compose.yml`, mêmes scripts. Il automatise les sections 1 à 4 de `livrables-client/Demarrage-du-lab-VM-pilote.md`.

Terraform ne traduit pas le lab en services Azure. Il crée une machine, et le lab y tourne dans Docker Compose : les TP, `incident.sh` et les notebooks fonctionnent donc sans adaptation.

État au 13/11/2026 : déployé une fois, en `spaincentral` avec une VM `Standard_D4s_v4`. La préparation de la VM s'est terminée par `OK` et `validate.sh` par « Environnement valide. » (détail dans `lab/TESTS.md`). Ce qui n'a pas encore été essayé est listé en fin de document.

## Sommaire

1. [Ce qui est créé](#1-ce-qui-est-créé)
2. [Contenu du dossier](#2-contenu-du-dossier)
3. [Prérequis](#3-prérequis)
4. [Déployer](#4-déployer)
5. [Se connecter au lab](#5-se-connecter-au-lab)
6. [Donner l'accès SSH au stagiaire](#6-donner-laccès-ssh-au-stagiaire)
7. [Travailler sur la VM](#7-travailler-sur-la-vm)
8. [Arrêter et redémarrer la VM](#8-arrêter-et-redémarrer-la-vm)
9. [Fin de session](#9-fin-de-session)
10. [Variables](#10-variables)
11. [Sorties](#11-sorties)
12. [Dépannage](#12-dépannage)
13. [Lab ou production](#13-lab-ou-production)
14. [Non testé](#14-non-testé)

## 1. Ce qui est créé

- Un groupe de ressources `kafka-lab-rg`, un réseau virtuel `10.42.0.0/24` et un sous-réseau `10.42.0.0/27`.
- Un groupe de sécurité qui n'autorise en entrée que SSH (port 22), depuis les adresses de `allowed_ssh_cidrs`.
- Une VM Ubuntu 24.04 x86_64 de 4 cœurs et 16 Go (`Standard_D4s_v5` par défaut, `Standard_D4s_v4` dans le déploiement actuel), disque de 64 Go, adresse publique fixe.
- Une clé SSH générée, écrite dans `.ssh/kafka-lab_id_rsa`.
- Un arrêt automatique quotidien à 19 h (heure de Paris), dans les régions où ce service existe.

Au premier démarrage, cloud-init lance `scripts/bootstrap.sh` sur la VM :

1. installation de Docker Engine et de Docker Compose depuis le dépôt officiel de Docker ;
2. ajout du compte `kafka` au groupe `docker` ;
3. clone du dépôt des supports dans `~/kafka-administration` ;
4. `prepare.sh` : téléchargement des images, génération des certificats propres à la VM ;
5. `.env` créé depuis `env.template`, avec `LAB_BIND_ADDRESS=127.0.0.1` et le jeton de JupyterLab (`JUPYTER_TOKEN`) ;
6. `start.sh extended`, puis `validate.sh`.

Les interfaces du lab n'écoutent que sur `127.0.0.1` dans la VM. On les atteint par un tunnel SSH, jamais par un port ouvert sur Internet.

## 2. Contenu du dossier

| Fichier | Rôle |
|---|---|
| `versions.tf` | Version de Terraform, providers `azurerm`, `tls` et `local` |
| `variables.tf` | Variables et leurs contrôles de validité |
| `network.tf` | Groupe de ressources, réseau, groupe de sécurité, adresse publique, carte réseau |
| `vm.tf` | Clé SSH, VM, cloud-init, arrêt automatique |
| `outputs.tf` | Adresse de la VM et commandes prêtes à lancer |
| `scripts/bootstrap.sh` | Préparation de la VM, exécutée une fois par cloud-init |
| `terraform.tfvars.example` | Modèle du fichier de valeurs |
| `.terraform.lock.hcl` | Versions exactes des providers, à suivre dans git |

Fichiers locaux, ignorés par git (`.gitignore` du dossier) :

| Fichier | Contenu | Précaution |
|---|---|---|
| `terraform.tfvars` | Abonnement, région, adresses IP autorisées | Propre à chaque poste |
| `terraform.tfstate` | État du déploiement, clé privée SSH comprise | Secret. Sans lui, Terraform ne sait plus ce qu'il a créé : ne pas le supprimer avant `terraform destroy` |
| `.ssh/kafka-lab_id_rsa` | Clé privée SSH de la VM | Secret |
| `.terraform/` | Providers téléchargés | Se régénère par `terraform init` |

## 3. Prérequis

- Terraform 1.9 ou plus, en version stable (testé avec 1.17.2). La préversion `1.14.0-alpha20250724` plante au chargement des providers récents.
- Azure CLI connecté (`az login`) et l'abonnement désigné : `export ARM_SUBSCRIPTION_ID=...` ou `subscription_id` dans `terraform.tfvars`.
- Un quota d'au moins 4 vCPU pour la famille de la VM dans la région. Le vérifier avant de déployer :

  ```bash
  az vm list-usage -l spaincentral -o table | grep -E "Standard DSv[45] Family|Total Regional vCPUs"    # colonnes : utilise, limite
  ```

  Sur l'abonnement du formateur, la famille DSv5 (taille par défaut) est à 0 en `francecentral` comme en `spaincentral`. `terraform.tfvars` fixe donc `location = "spaincentral"` et `vm_size = "Standard_D4s_v4"`, famille DSv4, quota de 10 vCPU.
- L'arrêt automatique n'existe pas dans toutes les régions. Il est absent de `spaincentral` : mettre alors `auto_shutdown_time = ""` et arrêter la VM à la main (section 8).

## 4. Déployer

```bash
cd infra/azure
cp terraform.tfvars.example terraform.tfvars   # renseigner allowed_ssh_cidrs
terraform init
terraform plan
terraform apply
```

Votre adresse IP publique, à mettre dans `allowed_ssh_cidrs` suivie de `/32` :

```bash
curl -s -4 https://ifconfig.me
```

`terraform plan` doit annoncer 11 ressources à créer (10 sans l'arrêt automatique). `terraform apply` demande une confirmation, puis rend la main dès que la VM existe.

La préparation de la VM continue ensuite toute seule : elle a pris 3 min 22 s le 13/11/2026 (Docker, images, démarrage et validation du lab). Pour attendre la fin et lire le résultat (attendu : `status: done`, puis `OK`) :

```bash
eval "$(terraform output -raw bootstrap_check_command)"
```

En cas d'échec, voir la section 12.

## 5. Se connecter au lab

`terraform output -raw` affiche une commande sans l'exécuter ; `eval` l'exécute. Le `%` que zsh ajoute à la fin de l'affichage ne fait pas partie de la commande.

Ouvrir un terminal sur la VM :

```bash
eval "$(terraform output -raw ssh_command)"
```

Ouvrir le tunnel vers les interfaces web :

```bash
eval "$(terraform output -raw tunnel_command)"
```

La commande reste bloquée sans rien afficher : le tunnel est ouvert tant qu'elle tourne. `Ctrl+C` le ferme.

Sur le poste du formateur, les ports locaux du tunnel sont préfixés par 1, pour ne pas les confondre avec ceux d'un lab qui tourne en local :

| Interface | Lab Azure (tunnel) | Lab local | Identifiants |
|---|---|---|---|
| JupyterLab | http://localhost:18888/?token=kafka-lab | http://localhost:8888 | jeton `kafka-lab` |
| Kafka UI | http://localhost:18080 | http://localhost:8080 | aucun |
| Grafana | http://localhost:13000 | http://localhost:3000 | `admin` / `kafka-lab` |
| Prometheus | http://localhost:19090 | http://localhost:9090 | aucun |

Sur la VM, rien ne change : les services écoutent toujours sur 8888, 8080, 3000 et 9090. Les ports Kafka (9092 à 9392) ne passent pas par le tunnel : les commandes Kafka se lancent sur la VM, dans le conteneur `tools` ou dans les notebooks.

### Jeton de JupyterLab et VS Code

Le JupyterLab de la VM est protégé par le jeton fixé par `jupyter_token` dans `terraform.tfvars` (`kafka-lab`). Le script de préparation l'écrit dans `~/kafka-administration/lab/.env` (`JUPYTER_TOKEN`) : il est donc repris à chaque `start.sh`, sans variable à repasser. L'adresse complète est donnée par :

```bash
terraform output -raw jupyter_url
```

Pour exécuter un notebook depuis VS Code, tunnel ouvert : *Select Kernel* > *Existing Jupyter Server...* > coller cette adresse > *Python 3 (ipykernel)*. Les précautions sont celles de `lab/jupyter/README.md` : le notebook ouvert est celui du poste, le code s'exécute sur le cluster de la VM, et enregistrer un corrigé après exécution remplace ses sorties réelles.

Changer `jupyter_token` dans `terraform.tfvars` ne modifie pas une VM existante. Sur celle-ci, modifier la ligne `JUPYTER_TOKEN=` de `lab/.env`, puis relancer `bash scripts/start.sh extended` : seul le conteneur `jupyter` est recréé.

## 6. Donner l'accès SSH au stagiaire

Le stagiaire a besoin de deux choses : que son adresse IP soit autorisée sur le port 22, et une clé SSH acceptée par la VM.

### 6.1 Autoriser son adresse IP

Lui demander son adresse IP publique, celle du réseau depuis lequel il suivra la formation. Il l'obtient avec :

```bash
curl -s -4 https://ifconfig.me
```

Ajouter cette adresse, suivie de `/32`, à la liste `allowed_ssh_cidrs` de `terraform.tfvars`, sans retirer la vôtre :

```hcl
allowed_ssh_cidrs = [
  "88.164.100.78/32", # formateur
  "198.51.100.25/32", # stagiaire
]
```

Puis appliquer :

```bash
terraform plan    # attendu : 1 to change (azurerm_network_security_group.lab), 0 to add, 0 to destroy
terraform apply
```

Seule la règle du groupe de sécurité est modifiée. La VM n'est ni recréée ni redémarrée et le lab continue de tourner. Si le plan propose de créer ou de détruire autre chose, ne pas appliquer et voir la section 12.

Si le stagiaire est derrière le réseau d'une entreprise, l'adresse à autoriser est celle de la sortie Internet de ce réseau ; pour une plage entière, utiliser sa notation CIDR (par exemple `198.51.100.0/24`). Si son adresse change (box redémarrée, partage de connexion), refaire cette étape avec la nouvelle adresse. Une plage en `/0` est refusée par la configuration.

### 6.2 Lui donner une clé SSH

Choisir une seule variante.

**Variante A : lui remettre la clé générée.** Transmettre le fichier `.ssh/kafka-lab_id_rsa` par un canal sûr (pas par courriel en clair). Vous partagez alors la même clé.

**Variante B : autoriser sa propre clé.** Il vous envoie sa clé publique (le fichier `.pub`, jamais la clé privée). L'ajouter sur la VM :

```bash
eval "$(terraform output -raw ssh_command)" \
  "echo 'ssh-ed25519 AAAA... stagiaire' >> ~/.ssh/authorized_keys"
```

La variable `extra_ssh_public_keys` ne sert qu'à la création de la VM : la modifier ensuite n'a aucun effet sur une VM existante.

### 6.3 Ce que le stagiaire lance

Lui communiquer l'adresse de la VM (`terraform output -raw public_ip`). Sur son poste, avec la clé reçue (variante A) :

```bash
chmod 600 kafka-lab_id_rsa
ssh -i kafka-lab_id_rsa kafka@ADRESSE_VM
```

Et pour les interfaces web, à laisser ouvert :

```bash
ssh -N -i kafka-lab_id_rsa \
  -L 8888:127.0.0.1:8888 -L 8080:127.0.0.1:8080 \
  -L 3000:127.0.0.1:3000 -L 9090:127.0.0.1:9090 \
  kafka@ADRESSE_VM
```

Le stagiaire garde les ports d'origine (http://localhost:8888, 8080, 3000, 9090) : il n'a pas de lab local et ce sont les adresses citées dans les énoncés des TP. JupyterLab lui demande le jeton à la première ouverture : lui donner l'adresse http://localhost:8888/lab?token=kafka-lab.

Avec sa propre clé (variante B), l'option `-i` est inutile. Sous Windows, ces commandes se lancent dans PowerShell, sans le `chmod`.

### 6.4 Retirer l'accès

Enlever son adresse de `allowed_ssh_cidrs`, puis `terraform apply`. En variante B, retirer aussi sa ligne de `~/.ssh/authorized_keys` sur la VM.

## 7. Travailler sur la VM

Les supports sont dans `~/kafka-administration`. Toutes les commandes du lab se lancent depuis `~/kafka-administration/lab`, comme en local (`lab/README.md`).

| Besoin | Commande, depuis `~/kafka-administration/lab` |
|---|---|
| Vérifier le lab | `bash scripts/validate.sh` |
| Passer en mode `full` | `bash scripts/start.sh full` |
| Poste d'administration | `docker compose exec tools bash` |
| Broker du TP7 | `docker compose --profile scale up -d kafka-4` |
| Cluster cloud du TP9 | `docker compose --profile mirror up -d kafka-cloud`, puis `docker compose --profile mirror up -d mirrormaker` |
| Repartir d'un lab vierge | `bash scripts/reset.sh --yes`, puis `bash scripts/start.sh extended` |
| Mettre à jour les supports | `git pull` dans `~/kafka-administration` |

`reset.sh` conserve le fichier `.env` : les ports restent liés à `127.0.0.1`. Le vérifier après toute intervention :

```bash
ss -ltn | grep -E ":(8080|8888|3000|9090|9092) "    # attendu : 127.0.0.1 sur chaque ligne
```

## 8. Arrêter et redémarrer la VM

Une VM allumée est facturée, qu'on s'en serve ou non. Le soir, la libérer :

```bash
az vm deallocate -g kafka-lab-rg -n kafka-lab-vm
```

`deallocate` arrête la facturation du calcul ; un simple arrêt depuis la VM (`sudo shutdown`) ne le fait pas. Le disque et l'adresse publique restent facturés tant que le déploiement existe.

Le lendemain :

```bash
az vm start -g kafka-lab-rg -n kafka-lab-vm
```

Les conteneurs du lab ne redémarrent pas seuls. Sur la VM :

```bash
cd ~/kafka-administration/lab && bash scripts/start.sh extended && bash scripts/validate.sh
```

L'adresse publique ne change pas, les données du lab non plus.

Dans une région où l'arrêt automatique existe, laisser `auto_shutdown_time = "1900"` : la VM est libérée chaque soir à 19 h, heure de Paris.

## 9. Fin de session

```bash
terraform destroy
```

Tout est supprimé : VM, disque, adresse publique, réseau, groupe de ressources. Les topics, les notebooks modifiés et tout ce qui a été fait sur la VM sont perdus : récupérer avant ce qui doit être gardé.

Vérifier ensuite qu'il ne reste rien :

```bash
az group exists -n kafka-lab-rg    # attendu : false
```

## 10. Variables

Les valeurs se mettent dans `terraform.tfvars`. Seule `allowed_ssh_cidrs` est obligatoire.

| Variable | Défaut | Rôle |
|---|---|---|
| `allowed_ssh_cidrs` | aucun | Adresses autorisées en SSH, en CIDR. `/0` refusé |
| `subscription_id` | `null` | Abonnement Azure ; `null` : lu dans `ARM_SUBSCRIPTION_ID` |
| `location` | `francecentral` | Région Azure |
| `vm_size` | `Standard_D4s_v5` | Taille de la VM : 4 cœurs, 16 Go, x86_64 |
| `os_disk_size_gb` | `64` | Disque système, 40 Go au minimum |
| `name_prefix` | `kafka-lab` | Préfixe des noms de ressources |
| `admin_username` | `kafka` | Compte Linux de la VM |
| `extra_ssh_public_keys` | `[]` | Clés publiques en plus de la clé générée, à la création seulement |
| `repo_url` | dépôt des supports | Dépôt cloné sur la VM, en lecture anonyme |
| `repo_ref` | `main` | Branche, tag ou commit installé ; un tag fige la version des supports |
| `lab_mode` | `extended` | Mode démarré à la création : `core`, `extended` ou `full` |
| `start_lab` | `true` | `false` : la VM est préparée, le lab n'est pas démarré |
| `jupyter_token` | chaîne vide | Jeton de JupyterLab, requis par VS Code ; vide : sans jeton. `terraform.tfvars` le fixe à `kafka-lab` |
| `auto_shutdown_time` | `1900` | Heure de l'arrêt quotidien (HHMM) ; chaîne vide : aucun arrêt |
| `auto_shutdown_timezone` | `Romance Standard Time` | Fuseau de l'arrêt, heure de Paris |
| `tags` | `{}` | Étiquettes ajoutées aux ressources |

Effet d'une modification sur un déploiement existant :

| Variable modifiée | Effet de `terraform apply` |
|---|---|
| `allowed_ssh_cidrs` | Règle du groupe de sécurité mise à jour, VM intacte |
| `auto_shutdown_time`, `tags` | Mise à jour en place |
| `vm_size` | VM redimensionnée, avec un redémarrage |
| `repo_ref`, `lab_mode`, `start_lab`, `jupyter_token`, `extra_ssh_public_keys` | Aucun : ces valeurs ne servent qu'à la création |
| `location`, `name_prefix`, `admin_username` | Tout est détruit et recréé. Passer par `terraform destroy` (section 12) |

Pour rejouer la préparation avec de nouvelles valeurs, ou après une modification de `scripts/bootstrap.sh`, recréer la VM seule. Tout ce qui a été fait sur la VM est perdu, l'adresse publique est conservée :

```bash
terraform apply -replace=azurerm_linux_virtual_machine.lab
```

## 11. Sorties

| Sortie | Contenu |
|---|---|
| `public_ip` | Adresse publique de la VM |
| `ssh_command` | Connexion SSH |
| `tunnel_command` | Tunnel vers les interfaces, ports locaux préfixés par 1 |
| `jupyter_url` | Adresse de JupyterLab par le tunnel, jeton compris ; à donner à VS Code |
| `bootstrap_check_command` | Attend la fin de la préparation et affiche son résultat |
| `ssh_private_key_path` | Emplacement de la clé privée |

Les chemins sont relatifs : lancer ces commandes depuis `infra/azure/`.

## 12. Dépannage

**`OperationNotAllowed [...] exceeding approved [...] Cores quota` à la création de la VM.** Le quota de la famille est insuffisant dans la région. Choisir une taille d'une famille qui a du quota (section 3), ou demander une augmentation dans le portail Azure, rubrique Quotas.

**`LocationNotAvailableForResourceType [...] Microsoft.DevTestLab/schedules`.** L'arrêt automatique n'existe pas dans la région : `auto_shutdown_time = ""`, puis `terraform apply`.

**`terraform plan` propose de créer une ressource qui existe déjà, ou la dit supprimée.** Rencontré le 13/11/2026 après un changement de `location` : le groupe de ressources, supprimé puis recréé sous le même nom dans une autre région, a provoqué des réponses « introuvable » intermittentes d'Azure, pour Terraform comme pour `az`. Ne pas appliquer. Relancer `terraform plan` jusqu'à obtenir deux fois de suite le résultat attendu. Pour changer de région sans ce problème : `terraform destroy`, attendre quelques minutes, modifier `location`, puis `terraform apply`.

**`a resource with the ID [...] already exists - to be managed via Terraform this resource needs to be imported`.** La ressource existe dans Azure mais pas dans l'état. L'importer avec l'identifiant donné par le message, puis relancer le plan :

```bash
terraform import azurerm_subnet.lab "IDENTIFIANT_DU_MESSAGE"
```

**SSH : `Operation timed out`.** Votre adresse IP a changé ou n'est pas dans `allowed_ssh_cidrs` (comparer avec `curl -s -4 https://ifconfig.me`), ou la VM est arrêtée :

```bash
az vm show -g kafka-lab-rg -n kafka-lab-vm -d --query powerState -o tsv    # attendu : VM running
```

**SSH : `Permission denied (publickey)`.** Mauvaise clé, ou compte autre que `kafka`.

**SSH : `REMOTE HOST IDENTIFICATION HAS CHANGED`.** La VM a été recréée derrière la même adresse. Oublier l'ancienne empreinte :

```bash
ssh-keygen -R "$(terraform output -raw public_ip)"
```

**Tunnel : `bind [127.0.0.1]:18888: Address already in use`.** Un tunnel est déjà ouvert dans un autre terminal, ou un autre programme occupe le port : `lsof -nP -iTCP:18888 -sTCP:LISTEN`.

**Le tunnel est ouvert mais une page ne répond pas.** Le lab est arrêté sur la VM (après un redémarrage de la VM, section 8). Sur la VM : `docker compose ps` depuis `~/kafka-administration/lab`.

**La préparation affiche `ECHEC` ou reste sur `EN COURS`.** Lire le journal sur la VM :

```bash
eval "$(terraform output -raw ssh_command)" "tail -40 /var/log/kafka-lab-bootstrap.log"
```

Le journal de cloud-init est dans `/var/log/cloud-init-output.log`. Après correction, soit relancer à la main les étapes restantes de la section 1, soit recréer la VM (section 10).

## 13. Lab ou production

| Réglage | Dans ce lab | En production |
|---|---|---|
| Accès | Adresse publique, SSH filtré par adresse source | Pas d'adresse publique : bastion ou VPN |
| Clé SSH | Générée par Terraform, présente dans `terraform.tfstate`, partagée avec le stagiaire | Clé personnelle fournie par chacun, état distant chiffré |
| État Terraform | Fichier local | Backend distant avec verrou |
| Compte Linux | Un seul compte, membre du groupe `docker`, donc équivalent root | Comptes nominatifs, droits séparés |
| Image de la VM | Version `latest` à la création | Version figée |
| Cluster | Trois nœuds dans une seule VM | Nœuds sur des machines et des zones distinctes |
| Interfaces web | Sans authentification, derrière un tunnel SSH | Authentification et chiffrement |
| Jeton de JupyterLab | Fixe (`kafka-lab`), en clair dans `terraform.tfvars` et `lab/.env` | Jeton généré, gardé secret |

Ne jamais ouvrir d'autre port que SSH sur cette VM : le compte `ANONYMOUS` est super-utilisateur du cluster et JupyterLab a accès au socket Docker, donc à toute la VM.

## 14. Non testé

Au 13/11/2026, ces points n'ont pas été essayés sur Azure :

- le tunnel SSH et les interfaces web depuis le poste du formateur ;
- la connexion de VS Code au JupyterLab de la VM (seul l'accès par jeton a été vérifié sur la VM : 403 sans jeton, 200 avec) ;
- l'écriture du jeton par `scripts/bootstrap.sh` à la création d'une VM (sur la VM actuelle, il a été ajouté à la main dans `lab/.env`) ;
- l'accès d'un second poste (stagiaire), variantes A et B ;
- `selftest.sh`, les notebooks des TP, le mode `full`, les profils `scale` et `mirror` ;
- `az vm deallocate`, `az vm start` et le redémarrage du lab qui suit ;
- le redimensionnement par `vm_size` et le remplacement de la VM par `-replace` ;
- `terraform destroy` ;
- l'arrêt automatique, absent de la région utilisée.

Reporter chaque essai dans `lab/TESTS.md`, avec sa date.
