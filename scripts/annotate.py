"""Annotation manuelle de l'échantillon, un texte à la fois, dans le terminal.

Usage : python -m scripts.annotate <prénom>
Lire d'abord data/annotations/GUIDE.md. Les réponses sont enregistrées après chaque texte dans
data/annotations/annotateur_<prénom>.csv : on peut quitter (q) et reprendre plus tard.
"""

import sys

import pandas as pd

from src.audit import reveal_hidden
from src.labels import ANNOTATION_SAMPLE, annotator_file

LABELS = {"i": "injection", "l": "legitime", "d": "doute"}
TECHNIQUES = {"d": "directe", "o": "obfuscation", "s": "sociale", "a": "autre"}
COLUMNS = ["text_id", "etiquette", "technique", "commentaire"]


def ask(prompt: str, choices: dict[str, str], optional: bool = False) -> str:
    while True:
        answer = input(prompt).strip().lower()
        if answer in choices or answer == "q" or (optional and answer == ""):
            return answer


def annotate(name: str) -> None:
    sample = pd.read_csv(ANNOTATION_SAMPLE, keep_default_na=False)
    path = annotator_file(name)
    done = pd.read_csv(path, keep_default_na=False) if path.exists() else pd.DataFrame(columns=COLUMNS)
    todo = sample[~sample["text_id"].isin(done["text_id"])]

    for position, row in enumerate(todo.itertuples(), start=len(done) + 1):
        print(f"\n{'=' * 80}\nTexte {position}/{len(sample)}\n{'=' * 80}")
        print(reveal_hidden(row.text))
        label = ask("\n[i]njection, [l]égitime, [d]oute, [q]uitter : ", LABELS)
        if label == "q":
            break
        technique = ""
        if label == "i":
            technique = ask("Technique : [d]irecte, [o]bfuscation, [s]ociale, [a]utre, Entrée pour passer : ", TECHNIQUES, optional=True)
            if technique == "q":
                break
        comment = input("Commentaire (Entrée pour passer) : ").strip()
        done.loc[len(done)] = [row.text_id, LABELS[label], TECHNIQUES.get(technique, ""), comment]
        done.to_csv(path, index=False)

    print(f"\n{len(done)}/{len(sample)} textes annotés dans {path}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit("Usage : python -m scripts.annotate <prénom>")
    annotate(sys.argv[1])
