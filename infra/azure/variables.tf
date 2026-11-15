variable "subscription_id" {
  description = "Identifiant de l'abonnement Azure. Laisser null pour utiliser ARM_SUBSCRIPTION_ID."
  type        = string
  default     = null
}

variable "location" {
  description = "Region Azure."
  type        = string
  default     = "francecentral"
}

variable "name_prefix" {
  description = "Prefixe des noms de ressources."
  type        = string
  default     = "kafka-lab"

  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{1,20}$", var.name_prefix))
    error_message = "Prefixe : minuscules, chiffres et tirets, 2 a 21 caracteres, commence par une lettre."
  }
}

variable "allowed_ssh_cidrs" {
  description = "Adresses autorisees a joindre le port 22 de la VM, en notation CIDR (exemple : [\"203.0.113.10/32\"]). Aucun autre port n'est ouvert."
  type        = list(string)

  validation {
    condition     = length(var.allowed_ssh_cidrs) > 0 && alltrue([for c in var.allowed_ssh_cidrs : can(cidrhost(c, 0))])
    error_message = "Donner au moins un CIDR valide."
  }

  validation {
    condition     = alltrue([for c in var.allowed_ssh_cidrs : !endswith(c, "/0")])
    error_message = "Ne pas ouvrir SSH a tout Internet : donner les adresses du formateur et du participant."
  }
}

variable "vm_size" {
  description = "Taille de la VM. Le lab demande 4 coeurs et 16 Go de RAM, en x86_64."
  type        = string
  default     = "Standard_D4s_v5"
}

variable "os_disk_size_gb" {
  description = "Taille du disque systeme en Go (40 Go conseilles au minimum)."
  type        = number
  default     = 64

  validation {
    condition     = var.os_disk_size_gb >= 40
    error_message = "Prevoir au moins 40 Go."
  }
}

variable "admin_username" {
  description = "Compte Linux de la VM, utilise par le participant et le formateur."
  type        = string
  default     = "kafka"

  validation {
    condition     = can(regex("^[a-z][a-z0-9]{1,30}$", var.admin_username)) && !contains(["admin", "root", "administrator"], var.admin_username)
    error_message = "Compte : minuscules et chiffres, commence par une lettre, hors noms reserves par Azure (admin, root)."
  }
}

variable "extra_ssh_public_keys" {
  description = "Cles publiques SSH supplementaires autorisees sur le compte, en plus de la cle generee par Terraform."
  type        = list(string)
  default     = []
}

variable "repo_url" {
  description = "Depot git des supports, clone sur la VM (lecture anonyme en HTTPS)."
  type        = string
  default     = "https://github.com/formateurdata/kafka-administration.git"

  validation {
    condition     = can(regex("^https://[A-Za-z0-9._/-]+$", var.repo_url))
    error_message = "URL HTTPS sans identifiants ni caracteres speciaux."
  }
}

variable "repo_ref" {
  description = "Branche, tag ou commit du depot a installer. Un tag ou un commit fige la version des supports."
  type        = string
  default     = "main"

  validation {
    condition     = can(regex("^[A-Za-z0-9._/-]+$", var.repo_ref))
    error_message = "Reference git invalide."
  }
}

variable "lab_mode" {
  description = "Mode du lab demarre au premier demarrage de la VM."
  type        = string
  default     = "extended"

  validation {
    condition     = contains(["core", "extended", "full"], var.lab_mode)
    error_message = "Mode attendu : core, extended ou full."
  }
}

variable "start_lab" {
  description = "Demarrer et valider le lab a la fin de la preparation. Sinon la VM est seulement preparee (images et certificats)."
  type        = bool
  default     = true
}

variable "jupyter_token" {
  description = "Jeton de JupyterLab, ecrit dans lab/.env sur la VM (JUPYTER_TOKEN). Requis pour brancher VS Code sur le serveur ; convention du lab : kafka-lab. Chaine vide : JupyterLab sans jeton."
  type        = string
  default     = ""

  validation {
    condition     = can(regex("^[A-Za-z0-9_-]*$", var.jupyter_token))
    error_message = "Jeton : lettres, chiffres, tirets et soulignes seulement."
  }
}

variable "auto_shutdown_time" {
  description = "Heure d'arret automatique quotidien de la VM, au format HHMM. Chaine vide : pas d'arret automatique."
  type        = string
  default     = "1900"

  validation {
    condition     = var.auto_shutdown_time == "" || can(regex("^([01][0-9]|2[0-3])[0-5][0-9]$", var.auto_shutdown_time))
    error_message = "Format attendu : HHMM, ou chaine vide."
  }
}

variable "auto_shutdown_timezone" {
  description = "Fuseau horaire de l'arret automatique (nom Windows)."
  type        = string
  default     = "Romance Standard Time"
}

variable "tags" {
  description = "Etiquettes ajoutees a toutes les ressources."
  type        = map(string)
  default     = {}
}
