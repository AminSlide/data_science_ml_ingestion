"""Tire l'échantillon de textes à annoter à la main dans data/annotations/echantillon.csv.

Usage : python -m scripts.sample_annotations
Le fichier est versionné : on ne le retire pas une fois l'annotation commencée.
"""

import sys

from src.config import INTERIM
from src.labels import ANNOTATION_SAMPLE, annotation_sample
from src.spark_session import get_spark

if __name__ == "__main__":
    if ANNOTATION_SAMPLE.exists():
        sys.exit(f"{ANNOTATION_SAMPLE} existe déjà : le supprimer à la main pour tirer un nouvel échantillon.")
    spark = get_spark("sample-annotations")
    texts = spark.read.parquet(str(INTERIM / "llmail_texts"))
    annotation_sample(texts).to_csv(ANNOTATION_SAMPLE, index=False)
    print(f"Échantillon écrit dans {ANNOTATION_SAMPLE}")
