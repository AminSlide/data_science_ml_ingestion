"""Construit data/interim/llmail_texts et llmail_submissions à partir des fichiers bruts.

Usage : python -m scripts.ingest_llmail  (après python -m scripts.download_data)
"""

from src.llmail import build_llmail, summarize
from src.spark_session import get_spark

if __name__ == "__main__":
    spark = get_spark("ingest-llmail")
    build_llmail(spark)
    summarize(spark)
