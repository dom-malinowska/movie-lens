from pyspark.sql import Window
from pyspark.sql.functions import col, collect_list, concat_ws, row_number

from movie_lens.movie_lens_utils import get_dataframe, create_agg_columns

# File config
FILE_OPTIONS = [
    {"delimiter": "::"}
]

FILE_DETAILS = [{"file_path": "data/ml-1m/movies.dat", "column_names": ["MovieID", "Title", "Genres"]},
                {"file_path": "data/ml-1m/ratings.dat", "column_names": ["UserID", "MovieID", "Rating", "Timestamp"]},
                {"file_path": "data/ml-1m/users.dat",
                 "column_names": ["UserID", "Gender", "Age", "Occupation", "Zip-code"]}]


def agg_movie_ratings(spark_session):
    # Initial dataframe load of each file
    movie_df = get_dataframe(spark_session=spark_session, config=FILE_DETAILS[0], file_options=FILE_OPTIONS)
    rating_df = get_dataframe(spark_session=spark_session, config=FILE_DETAILS[1], file_options=FILE_OPTIONS)

    # Ratings calculation
    ratings_agg_df = create_agg_columns(dataframe=rating_df, id_column="MovieID",
                                        column_operations={"max": "Rating", "min": "Rating", "avg": "Rating"})
    movies_with_rating = movie_df.join(ratings_agg_df, "MovieID", "left")

    movies_with_rating.show()


def agg_user_movies(spark_session):
    # Initial dataframe load of each file
    movie_df = get_dataframe(spark_session=spark_session, config=FILE_DETAILS[0], file_options=FILE_OPTIONS)
    rating_df = get_dataframe(spark_session=spark_session, config=FILE_DETAILS[1], file_options=FILE_OPTIONS)

    # Users top three movies calculation
    ratings_with_movie_title = rating_df.join(movie_df, "MovieID", "left")

    window_for_rating = Window.partitionBy("UserID").orderBy(col("Rating").desc())
    ranked_ratings = ratings_with_movie_title.withColumn("rank", row_number().over(window_for_rating))

    top_three_ranked_ratings = ranked_ratings.filter(ranked_ratings.rank <= 3)

    aggregated_users_with_movies = top_three_ranked_ratings.groupBy("UserID").agg(
        collect_list("Title").alias("TopMovies"))
    display_ready_users_with_movies = aggregated_users_with_movies.withColumn("TopMoviesList",
                                                                              concat_ws(", ", "TopMovies")).drop(
        "TopMovies")

    display_ready_users_with_movies.show(truncate=False)
