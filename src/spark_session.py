"""Création de la SparkSession, en un seul endroit pour tout le projet."""

import os
import sys
import warnings

from pyspark.sql import SparkSession


def get_spark(app_name: str = "injection-detector", driver_memory: str = "3g") -> SparkSession:
    # Sous Windows, Spark doit lancer ses workers avec le même Python que le notebook,
    # sinon il cherche un "python" quelconque dans le PATH.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    if os.name == "nt" and not os.environ.get("HADOOP_HOME"):
        warnings.warn(
            "HADOOP_HOME n'est pas défini : Spark pourra lire mais pas écrire de Parquet "
            "sous Windows (il faut winutils.exe et hadoop.dll dans %HADOOP_HOME%\\bin)."
        )

    return (
        SparkSession.builder.appName(app_name)
        # Mode local : tous les cœurs de la machine, pas de cluster.
        .master("local[*]")
        # La machine a ~8 Go de RAM : on laisse de la place au système et à Pandas.
        .config("spark.driver.memory", driver_memory)
        # 200 partitions par défaut, c'est fait pour un cluster ; en local, 8 suffisent.
        .config("spark.sql.shuffle.partitions", "8")
        # Arrow accélère toPandas(), createDataFrame(pandas_df) et les pandas_udf.
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
