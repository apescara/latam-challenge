from typing import List, Tuple

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, size, explode

APP_NAME = "LATAM DE Challenge - Q3"

def get_most_mentions(_spark: SparkSession, dataframe: DataFrame) -> DataFrame:

    # Obtener tweets con menciones
    dataframe = dataframe.select(col("quotedTweet.mentionedUsers").alias("mentions"))
    dataframe = dataframe.filter(size("mentions") > 0)

    # Extraer todas las menciones y separar contenido
    dataframe = dataframe.withColumn("mentions", explode("mentions"))
    dataframe = dataframe.select("mentions.*")

    # Calcular resultado final
    dataframe = (
        dataframe.groupBy(col("username")).count().sort("count", ascending=False).limit(10)
    )

    return dataframe

def q3_time(file_path: str) -> List[Tuple[str, int]]:
    spark = SparkSession.builder.appName(APP_NAME).getOrCreate()

    data = spark.read.json(file_path)
    dataframe = get_most_mentions(spark, data)

    del data

    output = [
        tuple(row) for row in dataframe.select([col("username"), col("count")]).collect()
    ]

    spark.stop()

    return output