from typing import List, Tuple

import emoji

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, udf, size, explode
from pyspark.sql.types import StringType, ArrayType, StructType, StructField

APP_NAME = "LATAM DE Challenge - Q2"


def get_most_used_emojis(_spark: SparkSession, dataframe: DataFrame) -> DataFrame:
    # define aux function to get the emojis from the text
    get_emojis = udf(
        lambda x: emoji.emoji_list(x),
        ArrayType(
            StructType(
                [
                    StructField("match_start", StringType(), True),
                    StructField("match_end", StringType(), True),
                    StructField("emoji", StringType(), True),
                ]
            )
        ),
    )

    # Extract emojis from content ann just keep that
    dataframe = dataframe.withColumn("emojis", get_emojis(col("content")))
    dataframe = dataframe.select("emojis")

    # Select only tweets with emojis and split
    dataframe = dataframe.filter(size(dataframe.emojis) > 0)
    dataframe = dataframe.withColumn("emojis", explode("emojis"))
    dataframe = dataframe.select("emojis.*")

    # Get most used ones
    dataframe = (
        dataframe.groupBy(col("emoji")).count().sort("count", ascending=False).limit(10)
    )

    return dataframe


def q2_time(file_path: str) -> List[Tuple[str, int]]:
    spark = SparkSession.builder.appName(APP_NAME).getOrCreate()

    data = spark.read.json(file_path)
    dataframe = get_most_used_emojis(spark, data)

    del data

    output = [
        tuple(row) for row in dataframe.select([col("emoji"), col("count")]).collect()
    ]

    spark.stop()

    return output