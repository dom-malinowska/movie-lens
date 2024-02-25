from pyspark.sql import SparkSession
from pyspark.sql import DataFrame


def create_spark_session(app_name: str, config: list[dict[str, str]] = None) -> SparkSession:
    builder = SparkSession.builder.appName(app_name)

    if config:
        for conf in config:
            for key, value in conf.items():
                builder.config(key, value)

    return builder.getOrCreate()


def load_dataframe(spark_session: SparkSession, file_location: str, options: list[dict[str, str]] = None) -> DataFrame:
    df_reader = spark_session.read

    if options:
        for opt in options:
            for key, value in opt.items():
                df_reader = df_reader.option(key, value)

    df = df_reader.csv(file_location)
    return df
