import unittest

from movie_lens.movie_lens_utils import create_agg_columns
from movie_lens.spark_utils import create_spark_session, load_dataframe


class TestMovieLensUtil(unittest.TestCase):

    def test_create_agg_columns_with_empty_dataframe(self):
        pass

    def test_create_agg_columns_with_numerical_input(self):
        sample_file = "test_file.csv"
        test_dataframe = load_dataframe(spark_session=create_spark_session(app_name="Test"), file_location=sample_file,
                                        options=[{"delimiter": ",", "header": "true"}]).toDF("title", "genre",
                                                                                             "year").limit(1)

        expected_results = {"max": 2031, "min": 2031, "avg": 2031.0}
        results = create_agg_columns(test_dataframe, {"max": "year", "min": "year", "avg": "year"})

        assert results == expected_results

    def test_create_agg_columns_with_non_numerical_input(self):
        pass
