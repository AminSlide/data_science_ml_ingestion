# Détection d'injections de prompt dans les e-mails

Projet de data science (Pandas et PySpark) : mesurer comment le nettoyage et le prétraitement des e-mails changent la capacité de modèles de ML classiques à détecter les injections de prompt, y compris dissimulées.

## Installation

### macOS

Prérequis : Python 3.10, Java 17. Spark 3.5 ne démarre pas avec un Java plus récent ; si `JAVA_HOME` n'est pas défini, `get_spark()` sélectionne Java 17 tout seul.

```bash
python3.10 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
```

### Windows

Prérequis : Python 3.10, Java 17 (`JAVA_HOME` défini).

```bash
python -m venv .venv
.venv\Scripts\python -m pip install -r requirements.txt
```

Pour que Spark puisse écrire du Parquet sous Windows, il faut `winutils.exe` et `hadoop.dll` dans `%HADOOP_HOME%\bin`.

## Données

Dans `data/raw/` (dossier non versionné) :

| Jeu | Source | Fichiers |
|---|---|---|
| LLMail-Inject | [Hugging Face, microsoft/llmail-inject-challenge](https://huggingface.co/datasets/microsoft/llmail-inject-challenge) (licence MIT), révision `1063bdf` | Téléchargés par `scripts/download_data.py` dans `data/raw/llmail/data/` (~2,5 Go). Étiquettes : `labelled_unique_submissions_phase{1,2}.json`. Métadonnées (équipe, niveau, dates, objectifs) : `raw_submissions_phase{1,2}.jsonl`. Mails légitimes : `emails_for_fp_tests.json`, `scenarios.json`. Descriptions : `levels_descriptions.json`, `objectives_descriptions.json`, `system_prompt.json` |
| Enron | [CMU](https://www.cs.cmu.edu/~enron/) ou version CSV sur Kaggle | à préciser (étape 4) |

## Pipeline

```bash
.venv/bin/python -m scripts.download_data   # LLMail-Inject, révision figée
.venv/bin/python -m scripts.ingest_llmail   # jointure étiquettes ↔ soumissions → data/interim/
.venv/bin/python -m pytest

# Étape 2 : audit (notebook) et annotation manuelle
.venv/bin/jupyter nbconvert --to notebook --execute --inplace notebooks/02_audit_etiquette.ipynb
.venv/bin/python -m scripts.sample_annotations   # échantillon de 200 textes, déjà tiré et versionné
.venv/bin/python -m scripts.annotate <prénom>    # guide : data/annotations/GUIDE.md
```

## Question de recherche et protocole

### Question

Dans quelle mesure le nettoyage et le prétraitement des e-mails modifient-ils la capacité de modèles de ML classiques (régression logistique, Naive Bayes, SVM linéaire, forêt aléatoire) à détecter les injections de prompt, en particulier les injections dissimulées ?

### Constats préalables sur les données

Mesurés à l'ingestion (`scripts/ingest_llmail.py`) sur les fichiers étiquetés :

| | Phase 1 | Phase 2 |
|---|---|---|
| Textes étiquetés uniques | 160 741 | 37 303 |
| `attack_attempt` true / false / unclear | 127 376 / 9 452 / 23 911 | 21 007 / 2 500 / 13 796 |
| Catégorie « obfuscation » selon le juge | 21 % | 12 % |
| Textes contenant `contact@contact.com` | 87 % | 81 % |
| Textes contenant « confirmation » | 79 % | 89 % |
| Textes dont les 150 premiers caractères (chiffres, espaces et casse normalisés) reviennent ailleurs | 63 % | 61 % |
| Équipes distinctes (auteur de la première soumission) | 220 | 98 |

- **L'étiquette n'est pas une vérité terrain.** `attack_attempt` vient soit de l'appel d'API déclenché (24 559 textes), soit d'un LLM juge (le reste). Le champ mélange chaînes et booléens. Deux valeurs aberrantes en phase 1 (une liste de votes, `1.0`) sont laissées vides à l'ingestion.
- **Les raccourcis sont massifs.** Tous les textes suivent le gabarit `Subject of the email: … Body: …`. Ajoutés à l'adresse et au mot cibles, ils suffisent à séparer LLMail de n'importe quel autre corpus.
- **Les textes « uniques » ne le sont qu'au caractère près.** En phase 2, on passe de 37 303 à 27 528 textes en normalisant chiffres, espaces et casse. 35 textes figurent dans les deux phases.
- **La jointure est presque complète.** 7 textes étiquetés n'ont pas de soumission correspondante (gabarit différent). 12 087 soumissions (2,6 %) sont absentes des fichiers d'étiquettes ; elles restent dans `llmail_submissions` avec `labelled = false`.
- **Peu de mails légitimes dans le même univers.** `emails_for_fp_tests.json` contient 203 mails, dont 160 distincts ; `scenarios.json` contient 78 mails de contexte distincts. Aucun recoupement entre les deux.

### Audit de l'étiquette (étape 2)

Détails et figures : `notebooks/02_audit_etiquette.ipynb`.

- **`unclear` veut dire ingénierie sociale.** Le juge classe 99 % des textes `unclear` en « ingénierie sociale », et 94 à 98 % des textes d'ingénierie sociale sont `unclear`. Écarter les `unclear` retire donc presque toute l'ingénierie sociale du corpus.
- **Le juge se trompe parfois de façon certaine.** 832 textes `false` ou `unclear` ont fait envoyer le mail à l'adresse de l'attaquant : ce sont des injections.
- **Le juge est stable sur ses décisions nettes.** Deux quasi-doublons reçoivent `true` et `false` dans moins de 0,3 % des paires. Les désaccords passent par `unclear`.
- **La production est très concentrée.** La première équipe écrit 23 % des textes en phase 1 et 27 % en phase 2 ; dix équipes en écrivent 68 % et 84 %. Une seule équipe écrit 63 % des `unclear` de la phase 2.
- **Les deux phases n'ont pas la même forme.**
  - Idéogrammes chinois : 22 % des textes en phase 1, 1 % en phase 2.
  - Faux marqueurs de rôle (`<|user|>`) : 32 % contre 6 %.
  - Tags Unicode invisibles : trente fois plus fréquents en phase 2.
- **La règle qui produit `api_triggered` ne se reproduit pas.** 4 452 textes passés par le juge ont aussi déclenché l'appel d'API.

### Hypothèses

Formulées avant les expériences :

- **H1** : la normalisation Unicode (NFKC, suppression des caractères invisibles) améliore le rappel sur les injections dissimulées sans augmenter le taux de faux positifs.
- **H2** : le prétraitement NLP classique (minuscules, mots vides, racinisation) dégrade la détection, car les injections reposent sur des mots outils et des marqueurs (« ignore all above », « you must », « SYSTEM: »).
- **H3** : les n-grammes de caractères résistent mieux à l'obfuscation que les n-grammes de mots.
- **H4** : supprimer les traces d'obfuscation fait perdre de l'information ; un nettoyage qui les conserve comme variables fait mieux que le nettoyage seul.
- **H5** : un modèle entraîné sur la phase 1 perd en performance sur la phase 2, où les attaquants se sont adaptés à de nouvelles défenses.

### Protocole

1. **Étiquette** (`binary_label` dans `src/labels.py`).
   - `attack_attempt = true` donne une injection, `false` une non-injection.
   - Un texte qui a fait envoyer le mail à l'adresse de l'attaquant est une injection, quelle que soit son étiquette.
   - Les `unclear` (ingénierie sociale) sont écartés par défaut. Règle fixée avant l'annotation : si au moins 90 % des `unclear` annotés sont des injections, ils deviennent positifs. Sinon, les compter positifs reste une analyse de sensibilité. Dans les deux cas, le rappel est mesuré à part sur l'ingénierie sociale.
   - La fiabilité du juge est estimée sur 200 textes annotés à la main :
     - à l'aveugle, l'annotateur ne voyant pas l'avis du juge ;
     - en strates selon le mécanisme d'étiquetage : 20 API, 60 `true`, 60 `unclear` et 60 `false` du juge ;
     - avec un seul texte par groupe de quasi-doublons.
     
     On calcule l'accord repondéré avec le juge, et le kappa de Cohen entre deux annotateurs.
2. **Négatifs et raccourcis.** Négatifs : mails Enron, textes LLMail `attack_attempt = false`, mails légitimes du challenge (`emails_for_fp_tests.json`, `scenarios.json`).
   - Les mails Enron sont mis au même gabarit, sans en-têtes, citations ni signatures.
   - Les adresses e-mail sont masquées dans les deux classes.
   - Les longueurs sont équilibrées.
   - Négatifs difficiles : mails Enron qui demandent d'envoyer ou de transférer quelque chose.
   - Témoin : un modèle qui ne voit que des variables de forme (longueur, ponctuation, nombre de lignes). S'il est très bon, le corpus contient encore un biais de source.
3. **Doublons et découpages.**
   - Les quasi-doublons sont regroupés avec `MinHashLSH` (Spark ML), et un groupe reste entier du même côté du découpage. Variante : découpage par équipe.
   - Découpage temporel : entraînement sur la phase 1, test sur la phase 2.
   - Le jeu de test est figé dès sa création et n'est utilisé qu'une fois, à la fin.
   - Contrôle : les résultats doivent tenir sans l'équipe la plus productive (23 à 27 % des textes).
4. **Nettoyage, le facteur étudié.** Chaque étape est testée seule à partir du texte brut (ablation) :
   - minuscules ;
   - normalisation NFKC ;
   - suppression des caractères invisibles ;
   - retrait du HTML ;
   - mots vides ;
   - racinisation.

   S'y ajoutent le nettoyage complet, et une variante « nettoyage complet + indicateurs d'obfuscation ».
5. **Injections dissimulées.**
   - Définition opérationnelle : `judge_category` contient « obfuscation », ou un détecteur maison se déclenche (caractères invisibles, tags Unicode U+E0000–E007F, homoglyphes, base64, texte espacé). La précision des détecteurs est vérifiée sur un échantillon.
   - Expérience contrôlée : les mêmes obfuscations sont appliquées aux injections du jeu de test, et on mesure la baisse du rappel pour chaque variante de nettoyage.
6. **Évaluation.**
   - Baselines : classe majoritaire, règles par mots-clés.
   - Métriques : PR-AUC, F1, rappel à 1 % de faux positifs, taux de faux positifs sur les mails légitimes du challenge et sur les négatifs difficiles, rappel sur les injections dissimulées.
   - Hyperparamètres réglés par validation croisée groupée sur l'entraînement.
   - Intervalles de confiance à 95 % par bootstrap ; test de McNemar pour comparer deux variantes sur le même jeu de test.
7. **Interprétation.**
   - Coefficients et importance par permutation, pour vérifier que les variables décisives ne sont pas des marqueurs de source.
   - Analyse des erreurs par technique (`judge_category`) et par défense.

### Limites

- Le challenge est artificiel : une adresse cible et un mot cible uniques, des consignes en anglais.
- Les mails Enron datent de 2000-2002 et viennent d'un autre domaine.
- Une partie des étiquettes est posée par un LLM.
- Enron contient des données personnelles : aucun mail brut n'est recopié dans le rapport.

### Avancement

- [x] 1. Téléchargement (révision figée) et jointure étiquettes ↔ métadonnées → `data/interim/`
- [x] 2. Audit qualité et analyse de l'étiquette (`notebooks/02_audit_etiquette.ipynb`)
- [ ] 2 bis. Annotation manuelle des 200 textes, puis choix de la politique pour `unclear`
- [ ] 3. Exploration : techniques par défense, LLM et phase, évolution dans le temps, succès des attaques
- [ ] 4. Corpus sans raccourcis : Enron, négatifs difficiles, témoin
- [ ] 5. Quasi-doublons et découpages, jeu de test figé
- [ ] 6. Fonctions de nettoyage testées, ablation
- [ ] 7. Features et modèles, comparés aux baselines
- [ ] 8. Évaluation et perturbations contrôlées
- [ ] 9. Interprétation, analyse d'erreurs, rapport

## Organisation

```
data/        raw → interim → processed (non versionné) ; annotations/ (versionné)
notebooks/   exploration et figures, appellent le code de src/
src/         code réutilisable : config, SparkSession, ingestion, audit, étiquette, figures
scripts/     points d'entrée pour relancer le pipeline
tests/       tests du code de src/
reports/     figures et rapport
conf/        configuration du journal de Spark
```
