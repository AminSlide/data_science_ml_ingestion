"""Télécharge LLMail-Inject depuis Hugging Face dans data/raw/llmail/.

Usage : python -m scripts.download_data
"""

from huggingface_hub import snapshot_download

from src.config import RAW

LLMAIL_REPO = "microsoft/llmail-inject-challenge"
# Révision figée (mai 2025) : si le jeu est modifié sur Hugging Face, on garde les mêmes données.
LLMAIL_REVISION = "1063bdf01ec8762b812d5e06ee768a06faa5a6f7"


def download_llmail() -> None:
    # ~2,5 Go, dont 1,7 Go pour raw_submissions_phase1.jsonl. Les fichiers déjà
    # téléchargés ne sont pas récupérés une seconde fois.
    snapshot_download(
        repo_id=LLMAIL_REPO,
        repo_type="dataset",
        revision=LLMAIL_REVISION,
        allow_patterns="data/*",
        local_dir=RAW / "llmail",
    )


if __name__ == "__main__":
    download_llmail()
