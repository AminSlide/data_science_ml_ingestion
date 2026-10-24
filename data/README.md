# Données

Ce dossier n'est pas versionné, à l'exception de ce fichier.

| Dossier | Contenu |
|---|---|
| `raw/` | Fichiers téléchargés, jamais modifiés |
| `interim/` | Parquet après ingestion et nettoyage |
| `processed/` | Jeux finaux utilisés par les modèles |

Les instructions de téléchargement sont dans le README à la racine du dépôt.

## Tables de `interim/`

Produites par `python -m scripts.ingest_llmail`, partitionnées par `phase` (1 ou 2).

**`llmail_texts/`** : une ligne par texte étiqueté unique, l'unité d'analyse des modèles. Clé : (`phase`, `text_id`), où `text_id` est l'empreinte SHA-256 du texte.

| Colonnes | Contenu |
|---|---|
| `text`, `subject`, `body` | Texte au gabarit `Subject of the email: … Body: …`, puis sujet et corps séparés (repris des soumissions) |
| `attack_attempt` | `true`, `false` ou `unclear` (vide pour 2 valeurs aberrantes) |
| `attack_attempt_raw` | Valeur d'origine en JSON (`"True"` chaîne ou `true` booléen) |
| `label_reason`, `judge_category`, `judge_answer` | Origine de l'étiquette (`api_triggered` ou `judge`), technique et justification du juge |
| `n_submissions`, `n_teams`, `first_team_id`, `first_seen`, `last_seen` | Soumissions de ce texte : nombre, équipes, auteur et date de la première, date de la dernière |
| `levels`, `success_any` | Niveaux où il a été soumis, réussite complète au moins une fois |

**`llmail_submissions/`** : une ligne par soumission, reliée à `llmail_texts` par `text_id`.

| Colonnes | Contenu |
|---|---|
| `job_id`, `team_id` | Identifiants de la soumission et de l'équipe |
| `level`, `scenario`, `llm`, `defense` | Niveau d'origine (ex. `level2v`), scénario (1 à 4), LLM et défense testés |
| `scheduled_time`, `started_time`, `completed_time` | Dates (UTC) |
| `email_retrieved`, `defense_undetected`, `exfil_sent`, `exfil_destination`, `exfil_content`, `success` | Objectifs atteints ; `success` si les cinq le sont |
| `labelled` | Faux pour les 2,6 % de soumissions absentes des fichiers d'étiquettes |
