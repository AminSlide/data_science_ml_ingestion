import json

import pytest
from pyspark.sql import functions as F

from src.llmail import email_text, email_text_col, normalize_attack_attempt, objectives_cols, read_levels


@pytest.mark.parametrize(
    "value, expected",
    [
        ("True", "true"),
        (True, "true"),
        ("False", "false"),
        (False, "false"),
        ("Unclear", "unclear"),
        # Valeurs aberrantes vues en phase 1.
        (["False", "True", "False", "False"], None),
        (1.0, None),
    ],
)
def test_normalize_attack_attempt(value, expected):
    assert normalize_attack_attempt(value) == expected


def test_read_levels(tmp_path):
    path = tmp_path / "levels_descriptions.json"
    path.write_text(json.dumps({"phase2": {"v": "GPT with all (Phi4 + Prompt Shield v2 + LLM Judge)"}}))
    assert read_levels(path) == [("v", "GPT", "all (Phi4 + Prompt Shield v2 + LLM Judge)")]


def test_email_text_col_matches_python(spark):
    # Le texte recalculé par Spark sert de clé de jointure : il doit être identique au caractère près,
    # y compris avec un séparateur recopié dans le sujet, des caractères invisibles ou un sujet vide.
    rows = [("Hello", "World"), ("", ""), ("Sujet.   Body: piège", "a\x00b​"), (None, "corps")]
    df = spark.createDataFrame(rows, "subject string, body string")
    got = [r.text for r in df.select(email_text_col(F.col("subject"), F.col("body")).alias("text")).collect()]
    assert got == [email_text(s or "", b or "") for s, b in rows]


def test_objectives_cols_reads_dotted_keys(spark):
    objectives = {
        "email.retrieved": True,
        "defense.undetected": False,
        "exfil.sent": True,
        "exfil.destination": False,
        "exfil.content": None,
    }
    df = spark.createDataFrame([(json.dumps(objectives),)], "objectives string")
    row = df.select(*objectives_cols(F.col("objectives"))).first()
    assert row.asDict() == {name.replace(".", "_"): value for name, value in objectives.items()}
