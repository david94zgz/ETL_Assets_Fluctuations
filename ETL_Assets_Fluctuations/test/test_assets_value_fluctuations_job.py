"""
test_assets_value_fuctuations_job.py
~~~~~~~~~~~~~~~

This module contains unit tests for the transformation steps of the ETL
job defined in assets_value_fuctuations_job.py. It makes use of a local version of 
PySpark that is bundled with the PySpark package.
"""

import unittest

from pyspark.sql import SparkSession
from pyspark.sql import types as T
import json

from jobs.assets_value_fluctuations_job import transform_data


path_to_config_file = "configs/assets_value_fluctuations_job_config.json"
with open(path_to_config_file, 'r') as config_file:
    config_dict = json.load(config_file)


class test_balances_negative_job(unittest.TestCase):
    """Test suite for transformation in etl_job.py
    """

    def setUp(self):
        """Start Spark, define config and path to test data
        """
        self.spark = (SparkSession
                            .builder
                            .appName("assets_value_fluctuations")
                            .getOrCreate())
        self.config_dict = config_dict
        self.test_data_path = 'data/test_data/'

    def tearDown(self):
        """Stop Spark
        """
        self.spark.stop()

    def test_transform_data(self):
        """Test data transformer.
        
        Using small chunks of input data and expected output data, we
        test the transformation step to make sure it's working as
        expected.
        """
        # Prepare an input data frame that mimics our source data.
        clients_input_schema = T.StructType([
            T.StructField("Id", T.IntegerType()),
            T.StructField("Name", T.StringType()),
            T.StructField("Surname", T.StringType())
        ])
        clients_df = (self.spark.read.format("csv")
            .option("header", "true")
            .schema(clients_input_schema)
            .load(self.test_data_path + 'test_clients_dataset.csv'))

        assets_input_schema = T.StructType([
            T.StructField("Id", T.IntegerType()),
            T.StructField("Client_Id", T.IntegerType()),
            T.StructField("Asset", T.StringType()),
            T.StructField("Market_Value", T.DoubleType()),
            T.StructField("Date_Market_Value", T.DateType())
        ])
        assets_df = (self.spark.read.format("csv")
            .option("header", "true")
            .schema(assets_input_schema)
            .load(self.test_data_path + 'test_assets_dataset.csv'))

        stocks_df = (self.spark.read.format("json")
            .option("multiline","true")
            .load(self.test_data_path + 'test_stocks_dataset.json'))
        stocks_df = stocks_df.withColumn("Date", stocks_df["Date"].cast(T.DateType()))
        stocks_df = stocks_df.withColumn("Interest_Rate", stocks_df["Interest_Rate"].cast(T.DoubleType()))
        stocks_df = stocks_df.withColumn("Stock_Price", stocks_df["Stock_Price"].cast(T.FloatType()))

        # Prepare an expected data frame which is the output that we expect.  
        expected_schema = T.StructType([
            T.StructField("Client_Id", T.IntegerType()),
            T.StructField("Name", T.StringType()),
            T.StructField("Surname", T.StringType()),
            T.StructField("Market_Value", T.DoubleType()),
            T.StructField("Current_Market_Value", T.DoubleType()),
            T.StructField("Absolute_Assets_Value_Fluctuation", T.DoubleType()),
            T.StructField("Percentage_Assets_Value_Fluctuation", T.DoubleType())
        ])
        expected_data_df = (self.spark.read.format("csv")
            .option("header", "true")
            .schema(expected_schema)
            .load(self.test_data_path + 'test_clients_assets_value_fluctuation.csv'))

        # Apply our transformation to the input data frame
        transformed_df = transform_data(clients_df, assets_df, stocks_df, self.config_dict)

        # Assert the output and data of the transformation to the expected data frame.
        field_list = lambda fields: (fields.name, fields.dataType, fields.nullable)
        transformed_field_list = [*map(field_list, transformed_df.schema.fields)]
        expected_field_list = [*map(field_list, expected_data_df.schema.fields)]
        
        identical_field_list = transformed_field_list == expected_field_list
        identical_data = sorted(expected_data_df.collect()) == sorted(transformed_df.collect())

        # Assert
        self.assertTrue(identical_field_list)
        self.assertTrue(identical_data)


if __name__ == '__main__':
    unittest.main()