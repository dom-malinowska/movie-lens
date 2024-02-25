from movie_lens.movie_lens import agg_movie_ratings, agg_user_movies
from movie_lens.spark_utils import create_spark_session

if __name__ == '__main__':
    # Spark config
    spark_session = create_spark_session(app_name="Movie_Lens")
    agg_movie_ratings(spark_session=spark_session)
    agg_user_movies(spark_session=spark_session)
