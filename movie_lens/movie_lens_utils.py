from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import avg, min, max

from movie_lens.spark_utils import load_dataframe


def get_dataframe(spark_session: SparkSession, config: dict, file_options: list) -> DataFrame:
    return load_dataframe(spark_session=spark_session, file_location=config["file_path"],
                          options=file_options).toDF(*config["column_names"])


def create_agg_columns(dataframe: DataFrame, column_operations: dict[str, str], id_column: str = None) -> DataFrame:
    return dataframe.groupBy(id_column).agg(max(column_operations["max"]).alias("max"),
                                            min(column_operations["min"]).alias("min"),
                                            avg(column_operations["avg"]).alias("avg")
                                            )
