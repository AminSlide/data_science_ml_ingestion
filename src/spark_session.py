"""Création de la SparkSession, en un seul endroit pour tout le projet."""

import os
import subprocess
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

    # Spark 3.5 ne démarre pas avec un Java récent (25 par exemple). Sous macOS, si
    # JAVA_HOME n'est pas défini, on prend Java 17 quand il est installé.
    if sys.platform == "darwin" and not os.environ.get("JAVA_HOME"):
        java17 = subprocess.run(["/usr/libexec/java_home", "-v", "17"], capture_output=True, text=True)
        if java17.returncode == 0:
            os.environ["JAVA_HOME"] = java17.stdout.strip()

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
