from typing import List, Tuple
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import to_date, col, udf
from pyspark.sql.types import StringType

APP_NAME = "LATAM DE Challenge - Q1"


def get_dates_and_users(_spark: SparkSession, dataframe: DataFrame) -> DataFrame:
    get_username = udf(lambda x: x["username"], StringType())

    dataframe = dataframe.withColumn("date_part", to_date(col("date")))
    dataframe = dataframe.withColumn("username", get_username(col("user")))

    dataframe_group = (
        dataframe.groupBy([col("date_part"), col("username")])
        .count()
        .sort("count", ascending=False)
    )

    dataframe_group = (
        _spark.sql(
            """
                SELECT
                    date_part,
                    username,
                    count,
                    SUM(count) OVER(PARTITION BY date_part) AS tweets_by_date,
                    RANK() OVER(PARTITION BY date_part ORDER BY count DESC) as rank_users
                FROM
                    {df}
              """,
            df=dataframe_group,
        )
        .filter("rank_users = 1")
        .sort("tweets_by_date", ascending=False)
        .limit(10)
    )
    return dataframe_group


def q1_memory(file_path: str) -> List[Tuple[datetime.date, str]]:

    spark = SparkSession.builder.appName(APP_NAME).getOrCreate()

    data = spark.read.json(file_path)
    dataframe = get_dates_and_users(spark, data)

    output = [
        tuple(row)
        for row in dataframe.select([col("date_part"), col("username")]).collect()
    ]

    spark.stop()

    return output
