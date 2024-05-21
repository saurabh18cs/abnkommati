import pytest
from chispa import assert_df_equality
from pyspark.sql import SparkSession
from job import extract, transform, rename, filter, save
from unittest.mock import patch
from collections import namedtuple

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.getOrCreate()

class MockArgs:
    def __init__(self, fpath_1, fpath_2):
        self.fpath_1 = fpath_1
        self.fpath_2 = fpath_2

@pytest.fixture(scope="module")
def test_client_df(spark):
    test_data = [
        (1, 'first_name1', 'last_name1', 'first_name1@test.test', 'Netherlands'),
        (2, 'first_name2', 'last_name2', 'first_name2@test.test', 'Netherlands'),
        (3, 'first_name3', 'last_name3', 'first_name3@test.test', 'United Kingdom'),
        (4, 'first_name4', 'last_name4', 'first_name4@test.test', 'France')
    ]
    return spark.createDataFrame(test_data, ["id", "first_name", "last_name", "email", "country"])

@pytest.fixture(scope="module")
def test_finance_df(spark):
    test_data = [
        (1, 'btc_a1', 'visa-electron', 12345678901),
        (2, 'btc_a2', 'jcb', 12345678902),
        (3, 'btc_a3', 'diners-club-enroute', 12345678903),
        (4, 'btc_a4', 'switch', 12345678904)
    ]
    return spark.createDataFrame(test_data, ["id", "btc_a", "cc_t", "cc_n"])

@patch.object(SparkSession, 'read')
def test_extract(mock_read, spark, test_finance_df, test_client_df):
    # Mock the read.csv calls to return the mock DataFrames
    mock_read.csv.side_effect = [test_client_df, test_finance_df]

    # Create a mock args object
    args = MockArgs('path/to/file1.csv', 'path/to/file2.csv')

    # Call the extract method
    extracted_df1, extracted_df2 = extract(spark, args)

    expected_data_client = spark.createDataFrame([(1, 'first_name1@test.test', 'Netherlands'),
                                           (2, 'first_name2@test.test', 'Netherlands'),
                                           (3, 'first_name3@test.test', 'United Kingdom'),
                                           (4, 'first_name4@test.test', 'France')], ["id", "email", "country"])
    expected_data_finance = spark.createDataFrame([
                                            (1, 'btc_a1', 'visa-electron'),
                                            (2, 'btc_a2', 'jcb'),
                                            (3, 'btc_a3', 'diners-club-enroute'),
                                            (4, 'btc_a4', 'switch')
                                        ], ["id", "btc_a", "cc_t"])
    # Assert the extracted DataFrames are equal to the mock DataFrames
    assert_df_equality(expected_data_client, extracted_df1)
    assert_df_equality(extracted_df2, expected_data_finance)

def test_transform(spark, test_finance_df, test_client_df):
    expected_data = [
                    (1, 'first_name1@test.test', 'Netherlands', 'btc_a1', 'visa-electron'),
                    (2, 'first_name2@test.test', 'Netherlands', 'btc_a2', 'jcb'),
                    (3, 'first_name3@test.test', 'United Kingdom', 'btc_a3', 'diners-club-enroute')]
    expected_df = spark.createDataFrame(expected_data, ["client_identifier", "email", "country", "bitcoin_address", "credit_card_type"])
    Args = namedtuple('Args', ['countries'])
    args = Args(['United Kingdom', 'Netherlands'])
    transformed_df = transform(test_client_df.select(['id', 'email', 'country']), test_finance_df.select(['id', 'btc_a', 'cc_t']), args)
    transformed_df.show()
    assert_df_equality(transformed_df, expected_df)