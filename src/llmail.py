"""Ingestion de LLMail-Inject : étiquettes et métadonnées des soumissions, en Parquet.

Le jeu est publié en deux morceaux qu'il faut recoller :
- labelled_unique_submissions_phase*.json : texte unique -> étiquette (attack_attempt,
  posée par l'appel d'API déclenché ou par un LLM juge) ;
- raw_submissions_phase*.jsonl : une ligne par soumission (équipe, niveau, dates,
  objectifs atteints), sans étiquette.

Le texte étiqueté est "Subject of the email: <sujet>.   Body: <corps>" : on le recalcule
côté soumissions et on joint sur son empreinte SHA-256 (text_id).

Sorties dans data/interim/, partitionnées par phase :
- llmail_texts : une ligne par texte unique, l'unité d'analyse des modèles ;
- llmail_submissions : une ligne par soumission, reliée aux textes par text_id.
"""

import json
import shutil
from functools import reduce
from pathlib import Path

import pandas as pd
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, StringType, StructField, StructType

from src.config import INTERIM, LLMAIL_RAW

PHASES = (1, 2)

EMAIL_PREFIX = "Subject of the email: "
EMAIL_SEPARATOR = ".   Body: "

OBJECTIVES = ("email.retrieved", "defense.undetected", "exfil.sent", "exfil.destination", "exfil.content")
OBJECTIVE_COLS = tuple(name.replace(".", "_") for name in OBJECTIVES)

# Seulement les champs utiles : "output" n'est qu'un écho du prompt, "Timestamp" est toujours vide
# et "RowKey" est égal à job_id.
SUBMISSION_SCHEMA = StructType(
    [
        StructField(name, StringType())
        for name in (
            "job_id", "team_id", "scenario", "subject", "body", "objectives",
            "scheduled_time", "started_time", "completed_time",
        )
    ]
)


def email_text(subject: str, body: str) -> str:
    """Texte tel qu'il apparaît dans les fichiers étiquetés (et dans le prompt du LLM)."""
    return f"{EMAIL_PREFIX}{subject}{EMAIL_SEPARATOR}{body}"


def email_text_col(subject: Column, body: Column) -> Column:
    """Équivalent Spark de email_text."""
    return F.concat(
        F.lit(EMAIL_PREFIX), F.coalesce(subject, F.lit("")),
        F.lit(EMAIL_SEPARATOR), F.coalesce(body, F.lit("")),
    )


def normalize_attack_attempt(value) -> str | None:
    # Le champ mélange des chaînes ("True", "False", "Unclear") et des booléens (true).
    # Les rares autres valeurs (liste de votes, 1.0) donnent une étiquette vide ; la valeur
    # d'origine reste dans attack_attempt_raw.
    label = str(value).strip().lower()
    return label if label in {"true", "false", "unclear"} else None


def _as_text(value) -> str | None:
    # judge_answer et judge_category sont presque toujours des chaînes, parfois des listes
    # (plusieurs avis) ou un nombre : on garde ces cas en JSON.
    if value is None or isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False)


def read_labels(path: Path, phase: int) -> pd.DataFrame:
    # Un seul objet JSON {texte: annotations} de plusieurs centaines de Mo : Spark le lirait
    # comme une ligne unique avec une colonne par texte, donc on passe par Python.
    with open(path, encoding="utf-8") as f:
        labelled = json.load(f)
    return pd.DataFrame(
        {
            "phase": phase,
            "text": text,
            "attack_attempt": normalize_attack_attempt(annotation["attack_attempt"]),
            # Valeur d'origine en JSON, pour garder la différence "True" (chaîne) / true (booléen).
            "attack_attempt_raw": json.dumps(annotation["attack_attempt"]),
            "label_reason": annotation["reason"],
            "judge_category": _as_text(annotation.get("judge_category")),
            "judge_answer": _as_text(annotation.get("judge_answer")),
        }
        for text, annotation in labelled.items()
    )


def read_levels(path: Path) -> list[tuple[str, str, str]]:
    """(lettre, LLM, défense) pour chaque lettre de niveau, ex. "v" -> GPT avec toutes les défenses."""
    levels = json.loads(path.read_text(encoding="utf-8"))
    return [
        (letter, *description.split(" with ", 1))
        for letters in levels.values()
        for letter, description in letters.items()
    ]


def objectives_cols(objectives: Column) -> list[Column]:
    """Les cinq objectifs, stockés en chaîne JSON, en colonnes booléennes."""
    parsed = F.from_json(objectives, StructType([StructField(name, BooleanType()) for name in OBJECTIVES]))
    # getField et pas parsed["a.b"] : les noms contiennent des points.
    return [parsed.getField(name).alias(col) for name, col in zip(OBJECTIVES, OBJECTIVE_COLS)]


def read_submissions(spark: SparkSession, raw_dir: Path = LLMAIL_RAW) -> DataFrame:
    """Une ligne par soumission, avec l'empreinte du texte pour la jointure avec les étiquettes.

    Le champ "scenario" d'origine vaut par exemple "level2v" : on le garde sous le nom "level"
    et on en tire le scénario (2) et la lettre (v), qui code le couple LLM x défense.
    """
    raw = reduce(
        DataFrame.unionByName,
        [
            spark.read.schema(SUBMISSION_SCHEMA)
            .json(str(raw_dir / f"raw_submissions_phase{phase}.jsonl"))
            .withColumn("phase", F.lit(phase))
            for phase in PHASES
        ],
    )
    submissions = raw.select(
        "phase",
        "job_id",
        "team_id",
        F.col("scenario").alias("level"),
        F.regexp_extract("scenario", r"^level(\d)[a-z]$", 1).cast("int").alias("scenario"),
        F.regexp_extract("scenario", r"^level\d([a-z])$", 1).alias("letter"),
        "subject",
        "body",
        *[F.col(c).cast("timestamp").alias(c) for c in ("scheduled_time", "started_time", "completed_time")],
        *objectives_cols(F.col("objectives")),
        F.sha2(email_text_col(F.col("subject"), F.col("body")), 256).alias("text_id"),
    )
    # Une attaque réussit si les cinq objectifs sont atteints.
    return submissions.withColumn("success", reduce(lambda a, b: a & b, [F.col(c) for c in OBJECTIVE_COLS]))


def build_llmail(spark: SparkSession, raw_dir: Path = LLMAIL_RAW, out_dir: Path = INTERIM) -> None:
    # Pas de "_" ni de "." en tête : Spark ignore ces dossiers, qu'il prend pour des fichiers techniques.
    staging = out_dir / "llmail_labels_staging"
    staging.mkdir(parents=True, exist_ok=True)
    for phase in PHASES:
        labels_pd = read_labels(raw_dir / f"labelled_unique_submissions_phase{phase}.json", phase)
        labels_pd.to_parquet(staging / f"phase{phase}.parquet", index=False)
        del labels_pd
    labels = spark.read.parquet(str(staging)).withColumn("text_id", F.sha2("text", 256))

    levels = spark.createDataFrame(read_levels(raw_dir / "levels_descriptions.json"), "letter string, llm string, defense string")
    submissions = (
        read_submissions(spark, raw_dir)
        .join(F.broadcast(levels), "letter", "left")
        .join(labels.select("phase", "text_id", F.lit(True).alias("labelled")), ["phase", "text_id"], "left")
        .fillna(False, subset=["labelled"])
    )

    per_text = submissions.groupBy("phase", "text_id").agg(
        # Sujet et corps séparés, qu'on ne peut pas retrouver à coup sûr depuis le texte :
        # certains attaquants ont recopié le séparateur dans leur sujet.
        F.min_by("subject", "scheduled_time").alias("subject"),
        F.min_by("body", "scheduled_time").alias("body"),
        F.count("*").alias("n_submissions"),
        F.size(F.collect_set("team_id")).alias("n_teams"),
        F.min_by("team_id", "scheduled_time").alias("first_team_id"),
        F.min("scheduled_time").alias("first_seen"),
        F.max("scheduled_time").alias("last_seen"),
        F.sort_array(F.collect_set("level")).alias("levels"),
        F.max("success").alias("success_any"),
        # Au moins une soumission a fait envoyer un mail à l'adresse de l'attaquant : le texte est
        # alors une injection à coup sûr, quelle que soit son étiquette.
        F.max(F.col("exfil_sent") & F.col("exfil_destination")).alias("exfil_target_any"),
    )
    # Jointure à gauche depuis les étiquettes : un texte sans soumission retrouvée est gardé
    # (métadonnées vides) pour qu'on puisse le compter.
    texts = labels.join(per_text, ["phase", "text_id"], "left")

    texts.write.mode("overwrite").partitionBy("phase").parquet(str(out_dir / "llmail_texts"))
    (
        submissions.drop("subject", "body", "letter")
        .write.mode("overwrite")
        .partitionBy("phase")
        .parquet(str(out_dir / "llmail_submissions"))
    )
    shutil.rmtree(staging)


def summarize(spark: SparkSession, out_dir: Path = INTERIM) -> None:
    """Contrôles de la jointure, relus depuis les Parquet écrits."""
    texts = spark.read.parquet(str(out_dir / "llmail_texts"))
    submissions = spark.read.parquet(str(out_dir / "llmail_submissions"))

    print("Jointure étiquettes <-> soumissions")
    (
        texts.groupBy("phase")
        .agg(
            F.count("*").alias("textes"),
            F.count("n_submissions").alias("textes_relies"),
        )
        .join(
            submissions.groupBy("phase").agg(
                F.count("*").alias("soumissions"),
                F.sum(F.col("labelled").cast("int")).alias("soumissions_etiquetees"),
                F.sum((F.col("llm").isNull() | F.col("scheduled_time").isNull()).cast("int")).alias("niveau_ou_date_illisible"),
            ),
            "phase",
        )
        .orderBy("phase")
        .show()
    )
    print("Étiquettes (attack_attempt) par phase")
    texts.groupBy("phase").pivot("attack_attempt").count().orderBy("phase").show()
