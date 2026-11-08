"""Verifications de la frontiere de livraison, sans moteur Docker."""
import hashlib
import importlib.util
import json
from pathlib import Path
import tarfile
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location("release_client", ROOT / "lab/scripts/release-client.py")
release = importlib.util.module_from_spec(spec)
spec.loader.exec_module(release)


class ReleaseTest(unittest.TestCase):
    def fixture(self, root):
        for name, data in release.selected_files(ROOT).items():
            path = root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(data)
        template = root / "lab/images/README-client.md"
        template.parent.mkdir(parents=True)
        template.write_bytes((ROOT / "lab/images/README-client.md").read_bytes())

    def test_archive_et_empreintes(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder) / "source"
            self.fixture(root)
            # Canaris : aucun de ces fichiers ne doit sortir du poste formateur.
            for name in ("lab/security/certs/ca.p12", "formateur/secret.md",
                         "tps/jour1/tp1/corrige.ipynb", "lab/data/private.txt",
                         "lab/monitoring/prometheus/rules/tp6-custom.yml"):
                path = root / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("NE_PAS_LIVRER")
            current = root / "lab/config/kafka/kafka-1/server.properties"
            current.write_text("incident.injecte=true")
            output = Path(folder) / "supports.tar.gz"
            release.build_archive(root, output, "test-1", "registry.example/lab/kafka:4.3.2-1",
                                  "registry.example/lab/jupyter:1")
            with tarfile.open(output) as archive:
                members = {entry.name.split("/", 1)[1]: archive.extractfile(entry).read()
                           for entry in archive.getmembers()}
            self.assertFalse(any(b"NE_PAS_LIVRER" in data for data in members.values()))
            self.assertNotIn(b"incident.injecte", members["lab/config/kafka/kafka-1/server.properties"])
            self.assertNotIn("lab/.env", members)
            self.assertNotIn(b"build:", members["lab/docker-compose.yml"])
            self.assertIn(b"registry.example/lab/kafka:4.3.2-1", members["lab/docker-compose.yml"])
            self.assertIn(b"JUPYTER_IMAGE=registry.example/lab/jupyter:1", members["lab/env.template"])
            manifest = json.loads(members.pop("livraison.json"))
            self.assertEqual(manifest["fichiers"], {
                name: hashlib.sha256(data).hexdigest() for name, data in members.items()})
            checksum = output.with_name(output.name + ".sha256").read_text().split()[0]
            self.assertEqual(checksum, hashlib.sha256(output.read_bytes()).hexdigest())

    def test_liens_refuses(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            self.fixture(root)
            path = root / "lab/data/orders.txt"
            path.unlink()
            path.symlink_to(ROOT / "lab/data/orders.txt")
            with self.assertRaisesRegex(ValueError, "symbolique"):
                release.selected_files(root)

    def test_tp_manquant_refuse(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            self.fixture(root)
            next(root.glob("tps/**/enonce.ipynb")).unlink()
            with self.assertRaisesRegex(ValueError, "huit TP"):
                release.selected_files(root)

    def test_versions_explicites(self):
        for value in ("repo/image:latest", "repo/image", "repo/image:1\nsecret"):
            with self.assertRaises(Exception):
                release.image_reference(value)
        self.assertEqual(release.image_reference("registry:5000/lab/kafka:4.3.2-1"),
                         "registry:5000/lab/kafka:4.3.2-1")


if __name__ == "__main__":
    unittest.main()
