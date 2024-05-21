import os
import sys
import argparse
from typing import Dict, List
import logging
from logging.handlers import RotatingFileHandler
import pandas as pd
from pyspark.sql import SparkSession, DataFrame, Column


__author__ = "Saurabh Gupta"
APP_NAME = "KommatiPara"


"""
    Setup global logger with rotating file strategy
"""
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler = RotatingFileHandler('job.log', maxBytes=2000, backupCount=2)
handler.setLevel(logging.DEBUG)
handler.setFormatter(formatter)
logger.addHandler(handler)


import argparse

def parser():
    """
    Parse command line arguments and return the parsed arguments.

    Returns:
        argparse.Namespace: The parsed command line arguments.
    """
    logger.info("Argument parser start")

    parser = argparse.ArgumentParser()
    parser.add_argument('--fpath_1', type=str, required=True,
                        help='Path to first file')
    parser.add_argument('--fpath_2', type=str, required=True,
                        help='Path to second file')
    parser.add_argument('--countries', type=str, default=[], 
                        nargs='+', required=True,
                        help='Countries to filter from the data')
    args = parser.parse_args()

    check_fpaths(args)
    logger.info("Parsed filepaths checked")
    check_countries(args)
    logger.info("Parsed countries checked")

    logger.info("Argument parser end")

    return args


def check_fpaths(args):
    """
    Checks if the parsed filepaths exist and throws an exception otherwise.

    :param args: Parsed arguments <class 'argparse.Namespace'>.
    :raises Exception: If the filepaths are invalid.
    """
    
    current_path = os.getcwd()

    fpath_1 = os.path.join(current_path, args.fpath_1)
    if not os.path.exists(fpath_1):
        raise Exception('Invalid path to first file')
    
    fpath_2 = os.path.join(current_path, args.fpath_2)
    if not os.path.exists(fpath_2):
        raise Exception('Invalid path to second file')
       

def check_countries(args):
    """ 
    Checks if parsed countries are present in the data and throws an exception otherwise.

    :param args: Parsed arguments <class 'argparse.Namespace'>.
    :type args: argparse.Namespace
    :raises Exception: If non-existent countries are selected.
    """

    pd_df1 = pd.read_csv(args.fpath_1, header=0)
    all_countries = pd_df1['country'].unique()

    if not (set(args.countries).issubset(all_countries)):
        raise Exception('Invalid countries selected, please choose from France, Netherlands, United Kingdom, United States')


def extract(spark, args) -> DataFrame:
    """
    Reads CSV files from arguments and creates Spark DataFrames.

    :param spark: The SparkSession object used for reading CSV files.
    :type spark: pyspark.sql.session.SparkSession

    :param args: The parsed arguments containing file paths.
    :type args: argparse.Namespace

    :return: Two DataFrames extracted from the CSV files.
    :rtype: Tuple[pyspark.sql.dataframe.DataFrame, pyspark.sql.dataframe.DataFrame]

    :raises: None
    """

    logger.info('Extraction start')

    df1 = spark.read.csv(args.fpath_1, header=True)
    df1 = df1.select(['id', 'email', 'country'])
    logger.info('DataFrame_1 extracted from CSV')

    df2 = spark.read.csv(args.fpath_2, header=True)
    df2 = df2.select(['id', 'btc_a', 'cc_t'])
    logger.info('DataFrame_2 extracted from CSV')

    logger.info('Extraction end')

    return df1, df2


def transform(df1: DataFrame, df2: DataFrame, args) -> DataFrame:
    """
    Transforms the given DataFrames by joining them, renaming columns, and filtering based on countries.

    :param df1: The first DataFrame to be joined.
    :type df1: DataFrame
    :param df2: The second DataFrame to be joined.
    :type df2: DataFrame
    :param args: Additional arguments.
    :type args: Any
    :return: The transformed DataFrame.
    :rtype: DataFrame
    """
    logger.info('Transformation start')

    col_names = {"id":"client_identifier", 
                 "btc_a":"bitcoin_address", 
                 "cc_t":"credit_card_type"}

    df = df1.join(df2, on='id', how='leftouter')
    logger.info("DataFrames joined")
    
    df = rename(df, col_names)
    logger.info("DataFrames columns renamed")

    df = filter(df, df.country, args.countries)
    logger.info("DataFrames filtered on countries")

    logger.info('Transformation end')

    return df


def rename(df: DataFrame, col_names: Dict) -> DataFrame:
    """ 
    Renames DataFrame columns with provided names

    :param df: DataFrame object representing the input DataFrame.
               It should be of type `pyspark.sql.dataframe.DataFrame`.
    :param col_names: Dictionary containing the current column names as keys
                      and the new column names as values.
                      It should be of type `dict`.
    :returns: DataFrame object with renamed columns.
              It is of type `pyspark.sql.dataframe.DataFrame`.
    :raises: This function does not raise any exceptions.
    """
    for col in col_names.keys():
        df = df.withColumnRenamed(col, col_names[col]) 
    
    return df


def filter(df: DataFrame, col_object: Column, values: List) -> DataFrame:
    """
    Filters a DataFrame based on the values in a specific column.

    Args:
        df (DataFrame): The input DataFrame to filter.
        col_object (Column): The column object representing the column to filter on.
        values (List): The list of values to filter on.

    Returns:
        DataFrame: The filtered DataFrame.

    Example:
        >>> df = spark.createDataFrame([(1, 'a'), (2, 'b'), (3, 'c')], ['id', 'value'])
        >>> col_object = df['value']
        >>> values = ['a', 'b']
        >>> filtered_df = filter(df, col_object, values)
        >>> filtered_df.show()
        +---+-----+
        | id|value|
        +---+-----+
        |  1|    a|
        |  2|    b|
        +---+-----+
    """
    return df.filter(col_object.isin(values))


def save(df: DataFrame):
    """
    Save the DataFrame as a CSV file in the 'client_data' directory.

    Args:
        df (DataFrame): The DataFrame to be saved.

    Returns:
        None
    """

    logger.info("Load start")

    current_path = os.getcwd()
    new_dir = 'client_data'
    new_path = os.path.join(current_path, new_dir)

    if not os.path.exists(new_path):
        os.mkdir(new_path)
        logger.info("Destination folder created")

    df.write.mode('overwrite').format('csv').options(header='True', delimiter=',').csv("client_data/result.csv")
    logger.info('DataFrame written to CSV')
    
    logger.info("Load end")


def main():
    """
    Entry point of the program.

    This function performs the following steps:
    1. Parses command line arguments using the `parser` function.
    2. Creates a Spark session.
    3. Extracts data from the specified sources using the `extract` function.
    4. Transforms the extracted data using the `transform` function.
    5. Displays the transformed data using the `show` method.
    6. Saves the transformed data using the `save` function.

    Returns:
        None
    """
    logger.info("Demo start")

    # Parsed arguments
    try:
        args = parser()
    except Exception as e:
        print(e)
        logger.error(e)
        return

    # Create Spark session
    spark = SparkSession.builder.appName(APP_NAME).getOrCreate()
    logger.info("Spark session created")

    # EXTRACT
    df1, df2 = extract(spark, args)

    # TRANSFORM
    df = transform(df1, df2, args)
    df.show()

    # LOAD
    save(df)

    logger.info("Demo end")


if __name__ == "__main__":
    main()