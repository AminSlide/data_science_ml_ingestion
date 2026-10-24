"""Cible binaire et annotation manuelle de l'étiquette (étape 2, notebook 02)."""

from functools import reduce
from math import sqrt
from pathlib import Path

import pandas as pd
from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F
from sklearn.metrics import cohen_kappa_score

from src.audit import near_dup_key
from src.config import ANNOTATIONS, SEED

# Valeur donnée aux textes "unclear", qui sont presque tous de l'ingénierie sociale.
UNCLEAR_POLICIES = {"exclude": None, "positive": 1, "negative": 0}

ANNOTATION_SAMPLE = ANNOTATIONS / "echantillon.csv"
# Strates = mécanisme qui a produit l'étiquette. false et unclear sont sur-représentés : rares, mais
# ce sont eux qui décident de la politique d'étiquette. Les estimations sont repondérées.
ANNOTATION_STRATA = {"api": 20, "juge_true": 60, "juge_unclear": 60, "juge_false": 60}
# Décision de l'API ou du juge, à comparer à celle des annotateurs (aucune pour unclear).
STRATUM_EXPECTED = {"api": "injection", "juge_true": "injection", "juge_false": "legitime", "juge_unclear": None}
# Règle fixée avant l'annotation : au moins 90 % d'injections parmi les unclear annotés et ils
# deviennent positifs ; sinon ils restent écartés, et "positive" reste une analyse de sensibilité.
UNCLEAR_THRESHOLD = 0.9


def binary_label(attack_attempt: Column, exfil_target_any: Column, unclear: str = "exclude") -> Column:
    """1 = injection, 0 = pas d'injection, nul = texte écarté.

    Un texte qui a fait envoyer un mail à l'adresse de l'attaquant est une injection, quoi qu'en
    dise le juge. Les autres suivent attack_attempt, et les "unclear" la politique choisie.
    """
    if unclear not in UNCLEAR_POLICIES:
        raise ValueError(f"politique inconnue pour unclear : {unclear!r} (choix : {', '.join(UNCLEAR_POLICIES)})")
    return (
        F.when(F.coalesce(exfil_target_any, F.lit(False)), 1)
        .when(attack_attempt == "true", 1)
        .when(attack_attempt == "false", 0)
        .when(attack_attempt == "unclear", F.lit(UNCLEAR_POLICIES[unclear]).cast("int"))
    )


def annotation_stratum(attack_attempt: Column, label_reason: Column) -> Column:
    return F.when(label_reason == "api_triggered", F.lit("api")).otherwise(F.concat(F.lit("juge_"), attack_attempt))


def annotation_sample(texts: DataFrame, sizes: dict[str, int] = ANNOTATION_STRATA, seed: int = SEED) -> pd.DataFrame:
    """Échantillon stratifié, un texte par groupe de quasi-doublons, présenté dans un ordre aléatoire.

    Le tirage est reproductible et ne dépend pas du partitionnement de Spark : l'aléa vient d'un
    hachage de text_id. La strate n'est pas renvoyée, pour que les annotateurs ne voient pas l'avis
    du juge ; on la retrouve par jointure sur text_id.
    """
    candidates = (
        texts.withColumn("stratum", annotation_stratum(F.col("attack_attempt"), F.col("label_reason")))
        .filter(F.col("stratum").isin(list(sizes)))
        .withColumn("draw", F.xxhash64("text_id", F.lit(seed)))
        .withColumn("in_group", F.row_number().over(Window.partitionBy("stratum", near_dup_key(F.col("text"))).orderBy("draw")))
        .filter(F.col("in_group") == 1)
        .withColumn("in_stratum", F.row_number().over(Window.partitionBy("stratum").orderBy("draw")))
    )
    keep = reduce(lambda a, b: a | b, [(F.col("stratum") == s) & (F.col("in_stratum") <= n) for s, n in sizes.items()])
    return (
        candidates.filter(keep)
        # Ordre de présentation : un autre hachage, pour mélanger les strates.
        .orderBy(F.xxhash64("text_id", F.lit(seed + 1)))
        .select("text_id", "text")
        .toPandas()
    )


def annotator_file(name: str) -> Path:
    return ANNOTATIONS / f"annotateur_{name}.csv"


def read_annotations() -> pd.DataFrame:
    """Toutes les annotations, une ligne par (annotateur, texte)."""
    files = sorted(ANNOTATIONS.glob("annotateur_*.csv"))
    if not files:
        return pd.DataFrame(columns=["text_id", "etiquette", "technique", "commentaire", "annotateur"])
    return pd.concat(
        [pd.read_csv(f, keep_default_na=False).assign(annotateur=f.stem.removeprefix("annotateur_")) for f in files],
        ignore_index=True,
    )


def wilson_interval(k: int, n: int, z: float = 1.96) -> tuple[float, float]:
    """Intervalle de confiance à 95 % de la proportion k/n (Wilson), fiable même pour de petits n."""
    if n == 0:
        return (float("nan"), float("nan"))
    p = k / n
    center = (p + z**2 / (2 * n)) / (1 + z**2 / n)
    half = z * sqrt(p * (1 - p) / n + z**2 / (4 * n**2)) / (1 + z**2 / n)
    return (center - half, center + half)


def human_label(annotations: pd.DataFrame) -> pd.Series:
    """Étiquette humaine de chaque texte : celle des annotateurs s'ils sont d'accord, sinon "desaccord"."""
    return annotations.groupby("text_id")["etiquette"].agg(lambda s: s.iloc[0] if s.nunique() == 1 else "desaccord")


def judge_agreement(
    annotations: pd.DataFrame, strata: pd.Series, stratum_sizes: dict[str, int]
) -> tuple[pd.DataFrame, float]:
    """Avis des annotateurs par strate, et accord juge-humains repondéré sur les décisions nettes.

    strata : strate de chaque text_id. stratum_sizes : taille de chaque strate dans la population
    échantillonnée (groupes de quasi-doublons), pour repondérer l'accord.
    """
    human = human_label(annotations).rename("humain").to_frame().join(strata.rename("strate"), how="inner")
    rows = []
    for stratum, group in human.groupby("strate"):
        n = len(group)
        counts = group["humain"].value_counts()
        low, high = wilson_interval(int(counts.get("injection", 0)), n)
        expected = STRATUM_EXPECTED[stratum]
        rows.append(
            {
                "strate": stratum,
                "n": n,
                **{label: counts.get(label, 0) / n for label in ("injection", "legitime", "doute", "desaccord")},
                "injection_ic95": f"[{low:.0%} ; {high:.0%}]",
                "accord_juge": (group["humain"] == expected).mean() if expected else float("nan"),
            }
        )
    table = pd.DataFrame(rows).set_index("strate")
    decisive = table["accord_juge"].dropna()
    weights = pd.Series({s: stratum_sizes[s] for s in decisive.index}, dtype=float)
    weighted = float((decisive * weights).sum() / weights.sum()) if len(decisive) else float("nan")
    return table, weighted


def annotator_kappa(annotations: pd.DataFrame) -> float | None:
    """Kappa de Cohen entre les deux premiers annotateurs, sur les textes qu'ils ont tous deux annotés."""
    names = sorted(annotations["annotateur"].unique())
    if len(names) < 2:
        return None
    wide = annotations.pivot_table(index="text_id", columns="annotateur", values="etiquette", aggfunc="first")
    both = wide.dropna(subset=names[:2])
    return cohen_kappa_score(both[names[0]], both[names[1]])


def unclear_policy(table: pd.DataFrame) -> str:
    """Politique pour les unclear d'après l'annotation (règle UNCLEAR_THRESHOLD)."""
    share = table["injection"].get("juge_unclear", float("nan"))
    return "positive" if share >= UNCLEAR_THRESHOLD else "exclude"
