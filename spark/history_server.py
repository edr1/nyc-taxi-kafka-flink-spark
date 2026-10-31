"""Run Spark History Server in the foreground so Docker manages its lifecycle."""
import os
from pathlib import Path

import pyspark

Path('/app/results/spark-events').mkdir(parents=True, exist_ok=True)
spark_class = Path(pyspark.__file__).resolve().parent / 'bin' / 'spark-class'
os.execv(str(spark_class), [str(spark_class), 'org.apache.spark.deploy.history.HistoryServer'])
