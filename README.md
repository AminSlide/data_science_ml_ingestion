# Détection d'injections de prompt dans les e-mails

Projet de data science (Pandas et PySpark) : mesurer comment le nettoyage et le prétraitement des e-mails changent la capacité de modèles de ML classiques à détecter les injections de prompt, y compris dissimulées.

## Installation (Windows)

Prérequis : Python 3.10, Java 17 (`JAVA_HOME` défini).

```bash
python -m venv .venv
.venv\Scripts\python -m pip install -r requirements.txt
```

Pour que Spark puisse écrire du Parquet sous Windows, il faut `winutils.exe` et `hadoop.dll` dans `%HADOOP_HOME%\bin` (voir plus bas).

## Données

À placer dans `data/raw/` (dossier non versionné) :

| Jeu | Source | Fichiers |
|---|---|---|
| LLMail-Inject | [Hugging Face, microsoft/llmail-inject-challenge](https://huggingface.co/datasets/microsoft/llmail-inject-challenge) (licence MIT) | `labelled_unique_submissions_phase1.json`, `labelled_unique_submissions_phase2.json`, `emails_for_fp_tests.json`, fichiers de description (`scenarios.json`, `levels_descriptions.json`, `objectives_descriptions.json`) |
| Enron | [CMU](https://www.cs.cmu.edu/~enron/) ou version CSV sur Kaggle | à préciser |

## Organisation

```
data/        raw → interim → processed (non versionné)
notebooks/   exploration et figures, appellent le code de src/
src/         code réutilisable : config, SparkSession, nettoyage, features, modèles
scripts/     points d'entrée pour relancer le pipeline
tests/       tests des fonctions de nettoyage
reports/     figures et rapport
```
