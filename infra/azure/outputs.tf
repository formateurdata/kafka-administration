output "public_ip" {
  description = "Adresse publique de la VM."
  value       = azurerm_public_ip.lab.ip_address
}

output "ssh_private_key_path" {
  description = "Cle privee SSH generee, a remettre au participant par un canal sur."
  value       = local_sensitive_file.ssh_private_key.filename
}

output "ssh_command" {
  description = "Connexion SSH a la VM."
  value       = "ssh -i ${local.private_key_path} ${var.admin_username}@${azurerm_public_ip.lab.ip_address}"
}

output "tunnel_command" {
  description = "Tunnel SSH vers JupyterLab, Kafka UI, Grafana et Prometheus. Laisser la commande ouverte. Les ports locaux sont prefixes par 1 (18888, 18080, 13000, 19090) pour ne pas se confondre avec ceux d'un lab local."
  value = join(" ", [
    "ssh -N -i ${local.private_key_path}",
    "-L 18888:127.0.0.1:8888",
    "-L 18080:127.0.0.1:8080",
    "-L 13000:127.0.0.1:3000",
    "-L 19090:127.0.0.1:9090",
    "${var.admin_username}@${azurerm_public_ip.lab.ip_address}",
  ])
}

output "jupyter_url" {
  description = "Adresse de JupyterLab a travers le tunnel du formateur, jeton compris. A donner aussi a VS Code (Existing Jupyter Server)."
  value       = var.jupyter_token == "" ? "http://localhost:18888/" : "http://localhost:18888/?token=${var.jupyter_token}"
}

output "bootstrap_check_command" {
  description = "Attend la fin de la preparation de la VM et affiche son resultat (attendu : OK)."
  value       = "ssh -i ${local.private_key_path} ${var.admin_username}@${azurerm_public_ip.lab.ip_address} 'cloud-init status --wait; cat /var/log/kafka-lab-bootstrap.status'"
}
