import unittest
from unittest.mock import MagicMock, patch

from pyspark.sql import SparkSession, DataFrame

from movie_lens.spark_utils import load_dataframe


class TestSparkUtils(unittest.TestCase):

    def setUp(self):
        self.spark_session_mock = MagicMock(spec=SparkSession)
        self.df_reader_mock = MagicMock()
        self.spark_session_mock.read = self.df_reader_mock

    def test_create_spark_session_without_config(self):
        pass

    def test_load_dataframe_no_options(self):
        expected_df = MagicMock(spec=DataFrame)
        self.df_reader_mock.csv.return_value = expected_df

        file_location = "test_file.csv"
        with patch.object(SparkSession, 'builder', return_value=self.spark_session_mock):
            df = load_dataframe(self.spark_session_mock, file_location)

        self.df_reader_mock.csv.assert_called_once_with(file_location)
        self.assertEqual(df, expected_df)

    def test_load_dataframe_with_options(self):
        pass
