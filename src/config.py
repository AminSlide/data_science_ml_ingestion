"""Chemins et constantes partagés par tout le projet."""

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

DATA = ROOT / "data"
RAW = DATA / "raw"
INTERIM = DATA / "interim"
PROCESSED = DATA / "processed"
FIGURES = ROOT / "reports" / "figures"
# Annotations manuelles : versionnées, contrairement au reste de data/.
ANNOTATIONS = DATA / "annotations"

# snapshot_download garde l'arborescence du dépôt Hugging Face (dossier data/).
LLMAIL_RAW = RAW / "llmail" / "data"

SEED = 42

for folder in (RAW, INTERIM, PROCESSED, FIGURES, ANNOTATIONS):
    folder.mkdir(parents=True, exist_ok=True)
