"""Chemins et constantes partagés par tout le projet."""

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

DATA = ROOT / "data"
RAW = DATA / "raw"
INTERIM = DATA / "interim"
PROCESSED = DATA / "processed"
FIGURES = ROOT / "reports" / "figures"

SEED = 42

for folder in (RAW, INTERIM, PROCESSED, FIGURES):
    folder.mkdir(parents=True, exist_ok=True)
