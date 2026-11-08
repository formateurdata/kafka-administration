#!/usr/bin/env python3
"""Archive des supports participants, construite depuis une liste fermee."""
import argparse
import hashlib
import io
import json
from pathlib import Path
import re
import tarfile

ROOT = Path(__file__).resolve().parents[2]


def selected_files(root):
    files = {}
    patterns = (
        "lab/config/kafka/pristine/kafka-*.server.properties", "tps/**/enonce.md", "tps/**/enonce.ipynb",
        "demos-tp/*.ipynb", "livrables-client/*.pdf",
    )
    exact = (
        "lab/docker-compose.yml", "lab/env.template", "lab/config/kafka/pristine/log4j2.yaml",
        "lab/clients/alice.properties", "lab/clients/admin.properties",
        "lab/clients/plaintext.properties", "lab/clients/secure-template.properties",
        "lab/config/connect/connect-distributed.properties",
        "lab/security/generate-certs.sh", "lab/jupyter/labkafka.py",
        "lab/jupyter/jupyter_server_config.py",
        "lab/monitoring/jmx-exporter/kafka-broker.yml",
        "lab/monitoring/jmx-exporter/kafka-client.yml",
        "lab/monitoring/prometheus/prometheus.yml",
        "lab/monitoring/prometheus/rules/kafka-alerts.yml",
        "lab/monitoring/grafana/provisioning/dashboards/kafka.yml",
        "lab/monitoring/grafana/provisioning/datasources/prometheus.yml",
        "lab/monitoring/grafana/dashboards/kafka-cluster.json",
        "lab/data/orders.txt", "lab/data/product-prices.txt", "lab/data/clickstream-source.txt",
    )
    scripts = ("lib", "prepare", "start", "stop", "reset", "validate", "incident", "offline-bundle")
    paths = [root / name for name in exact]
    paths += [root / f"lab/scripts/{name}.sh" for name in scripts]
    for pattern in patterns:
        paths.extend(root.glob(pattern))
    for path in sorted(set(paths)):
        relative = path.relative_to(root)
        # Une liste fermee n'autorise pas un lien vers un autre fichier.
        if path.is_symlink() or path.resolve() != root.resolve() / relative:
            raise ValueError(f"Lien symbolique refuse : {relative}")
        if not path.is_file():
            raise ValueError(f"Fichier requis absent : {relative}")
        files[relative.as_posix()] = path.read_bytes()
    # Une session en cours ne doit pas contaminer la livraison.
    for node in range(1, 5):
        files[f"lab/config/kafka/kafka-{node}/server.properties"] = files[
            f"lab/config/kafka/pristine/kafka-{node}.server.properties"]
    files["lab/config/kafka/common/log4j2.yaml"] = files["lab/config/kafka/pristine/log4j2.yaml"]
    for suffix in ("/enonce.md", "/enonce.ipynb"):
        if sum(name.endswith(suffix) for name in files) != 8:
            raise ValueError(f"Les huit TP sont requis : {suffix}")
    if sum(name.startswith("demos-tp/") for name in files) != 8:
        raise ValueError("Les huit demonstrations sont requises.")
    return files


def image_reference(value):
    if not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9._/:@-]+", value):
        raise argparse.ArgumentTypeError("Reference d'image invalide.")
    if ":" not in value.rsplit("/", 1)[-1] or value.endswith(":latest"):
        raise argparse.ArgumentTypeError("Une version explicite est requise (pas latest).")
    return value


def build_archive(root, output, version, kafka_image, jupyter_image):
    files = selected_files(root)
    compose = files["lab/docker-compose.yml"].decode()
    compose = re.sub(r"\$\{KAFKA_IMAGE:-[^}]+\}",
                     lambda _: "${KAFKA_IMAGE:-" + kafka_image + "}", compose)
    compose = re.sub(r"\$\{JUPYTER_IMAGE:-[^}]+\}",
                     lambda _: "${JUPYTER_IMAGE:-" + jupyter_image + "}", compose)
    files["lab/docker-compose.yml"] = compose.encode()
    template = re.sub(r"(?m)^KAFKA_IMAGE=.*$", lambda _: "KAFKA_IMAGE=" + kafka_image,
                      files["lab/env.template"].decode())
    template = re.sub(r"(?m)^JUPYTER_IMAGE=.*$", lambda _: "JUPYTER_IMAGE=" + jupyter_image, template)
    files["lab/env.template"] = template.encode()
    files["README.md"] = (root / "lab/images/README-client.md").read_bytes().replace(
        b"@@VERSION@@", version.encode())
    files["lab/README.md"] = "Voir [le guide d'installation](../README.md).\n".encode()
    manifest = {
        "version": version,
        "images": {"kafka": kafka_image, "jupyter": jupyter_image},
        "plateformes": ["linux/amd64", "linux/arm64"],
        "fichiers": {name: hashlib.sha256(data).hexdigest() for name, data in sorted(files.items())},
    }
    files["livraison.json"] = (json.dumps(manifest, ensure_ascii=False, indent=2) + "\n").encode()
    output.parent.mkdir(parents=True, exist_ok=True)
    if output.exists():
        raise ValueError(f"Archive deja presente : {output}")
    with tarfile.open(output, "w:gz") as archive:
        for name, data in sorted(files.items()):
            entry = tarfile.TarInfo(f"Kafka-Administration-{version}/{name}")
            entry.size = len(data)
            entry.mode = 0o755 if name.endswith(".sh") else 0o644
            archive.addfile(entry, io.BytesIO(data))
    checksum = hashlib.sha256(output.read_bytes()).hexdigest()
    output.with_name(output.name + ".sha256").write_text(f"{checksum}  {output.name}\n")
    return len(files)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--version", required=True)
    parser.add_argument("--kafka-image", required=True, type=image_reference)
    parser.add_argument("--jupyter-image", required=True, type=image_reference)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    if not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9._-]*", args.version):
        parser.error("Version invalide.")
    count = build_archive(ROOT, args.output, args.version, args.kafka_image, args.jupyter_image)
    print(f"Archive creee : {args.output} ({count} fichiers), empreinte .sha256 jointe.")


if __name__ == "__main__":
    main()
