locals {
  tags = merge({
    projet = "formation-kafka-administration"
    gere   = "terraform"
  }, var.tags)

  private_key_path = "${path.module}/.ssh/${var.name_prefix}_id_rsa"

  # Parametres lus par scripts/bootstrap.sh. Les variables sont validees
  # (aucune apostrophe possible), les valeurs sont donc citees sans risque.
  bootstrap_env = join("\n", [
    "LAB_USER='${var.admin_username}'",
    "REPO_URL='${var.repo_url}'",
    "REPO_REF='${var.repo_ref}'",
    "LAB_MODE='${var.lab_mode}'",
    "START_LAB='${var.start_lab ? 1 : 0}'",
    "JUPYTER_TOKEN='${var.jupyter_token}'",
    "",
  ])

  cloud_config = {
    package_update = true
    packages       = ["ca-certificates", "curl", "git"]
    write_files = [
      {
        path        = "/etc/kafka-lab/bootstrap.env"
        permissions = "0644"
        content     = local.bootstrap_env
      },
      {
        path        = "/opt/kafka-lab/bootstrap.sh"
        permissions = "0755"
        content     = file("${path.module}/scripts/bootstrap.sh")
      },
    ]
    runcmd = [
      ["bash", "/opt/kafka-lab/bootstrap.sh"],
    ]
  }
}

# Cle SSH generee pour la VM. La cle privee est aussi dans l'etat Terraform :
# traiter terraform.tfstate comme un secret.
resource "tls_private_key" "ssh" {
  algorithm = "RSA"
  rsa_bits  = 4096
}

resource "local_sensitive_file" "ssh_private_key" {
  filename        = local.private_key_path
  content         = tls_private_key.ssh.private_key_openssh
  file_permission = "0600"
}

resource "azurerm_linux_virtual_machine" "lab" {
  name                  = "${var.name_prefix}-vm"
  location              = azurerm_resource_group.lab.location
  resource_group_name   = azurerm_resource_group.lab.name
  size                  = var.vm_size
  admin_username        = var.admin_username
  network_interface_ids = [azurerm_network_interface.lab.id]
  tags                  = local.tags

  disable_password_authentication = true

  admin_ssh_key {
    username   = var.admin_username
    public_key = tls_private_key.ssh.public_key_openssh
  }

  os_disk {
    name                 = "${var.name_prefix}-osdisk"
    caching              = "ReadWrite"
    storage_account_type = "StandardSSD_LRS"
    disk_size_gb         = var.os_disk_size_gb
  }

  # Ubuntu 24.04 LTS x86_64 : systeme sur lequel le lab a ete teste.
  source_image_reference {
    publisher = "Canonical"
    offer     = "ubuntu-24_04-lts"
    sku       = "server"
    version   = "latest"
  }

  custom_data = base64encode("#cloud-config\n${yamlencode(merge(local.cloud_config, {
    users = [
      "default",
      {
        name                = var.admin_username
        ssh_authorized_keys = concat([tls_private_key.ssh.public_key_openssh], var.extra_ssh_public_keys)
      },
    ]
  }))}")

  # Console serie et captures de demarrage, utiles si cloud-init echoue.
  boot_diagnostics {}

  depends_on = [azurerm_subnet_network_security_group_association.lab]

  lifecycle {
    # Une nouvelle image "latest" ou une modification du script ne doit pas
    # detruire une VM en cours de formation. Pour rejouer la preparation :
    # terraform apply -replace=azurerm_linux_virtual_machine.lab
    ignore_changes = [source_image_reference, custom_data]
  }
}

resource "azurerm_dev_test_global_vm_shutdown_schedule" "lab" {
  count = var.auto_shutdown_time == "" ? 0 : 1

  virtual_machine_id    = azurerm_linux_virtual_machine.lab.id
  location              = azurerm_resource_group.lab.location
  enabled               = true
  daily_recurrence_time = var.auto_shutdown_time
  timezone              = var.auto_shutdown_timezone
  tags                  = local.tags

  notification_settings {
    enabled = false
  }
}
