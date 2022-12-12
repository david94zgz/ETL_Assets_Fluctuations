"""
assets_value_fluctuations_job.py
~~~~~~~~~~

This Python module contains an example Apache Spark ETL job definition.
 It can be submitted to a Spark cluster (or locally) using the 'spark-submit'
command found in the '/bin' directory of all Spark distributions
(necessary for running any Spark job, locally or otherwise). For
example, this example script can be executed as follows,
    $SPARK_HOME/bin/spark-submit \
    --py-files packages.zip \
    --files configs/assets_value_fluctuations_job_config.json \
    jobs/assets_value_fluctuations_job.py
where packages.zip contains Python modules required by ETL job (in
this example it contains a class to provide access to Spark's logger),
which need to be made available to each executor process on every node
in the cluster; assets_value_fluctuations_job_config_config.json is a text 
file sent to the cluster, containing a JSON object with all of the configuration 
parameters required by the ETL job; and, assets_value_fluctuations_job.py 
contains the Spark application to be executed by a driver process on the Spark 
master node.
The chosen approach for structuring jobs is to separate the individual
'units' of ETL - the Extract, Transform and Load parts - into dedicated
functions, such that the key Transform steps can be covered by tests
and jobs or called from within another environment (e.g. a Jupyter or
Zeppelin notebook).
"""


from pyspark.sql.functions import when, round, col, lit, datediff, current_date

from pyspark.sql import types as T

from dependencies.spark import start_spark


def main():
    """Main ETL script definition.

    :return: None
    """
    # start Spark application and get Spark session, logger and config
    spark, log, config = start_spark(
            app_name='assets_value_fluctuations',
            files=["configs/assets_value_fluctuations_job_config.json"])
    

    # log that main ETL job is starting
    log.warn('assets_value_fluctuations is up-and-running')

    # execute ETL pipeline
    clients_df, assets_df, stocks_df = extract_data(spark)
    data_transformed = transform_data(clients_df, assets_df, stocks_df, config)
    load_data(data_transformed)

    # log the success and terminate Spark application
    log.warn('assets_value_fluctuations is finished')
    spark.stop()
    return None


def extract_data(spark):
    """Load data from CSV and JSON file format.

    :param spark: Spark session.
    :return: clients, assets and stocks Spark DataFrame.
    """
    clients_input_schema = T.StructType([
        T.StructField("Id", T.IntegerType()),
        T.StructField("Name", T.StringType()),
        T.StructField("Surname", T.StringType())
    ])
    clients_df = (spark.read.format("csv")
        .option("header", "true")
        .schema(clients_input_schema)
        .load('data/clients_dataset.csv'))

    assets_input_schema = T.StructType([
        T.StructField("Id", T.IntegerType()),
        T.StructField("Client_Id", T.IntegerType()),
        T.StructField("Asset", T.StringType()),
        T.StructField("Market_Value", T.DoubleType()),
        T.StructField("Date_Market_Value", T.DateType())
    ])
    assets_df = (spark.read.format("csv")
        .option("header", "true")
        .schema(assets_input_schema)
        .load('data/assets_dataset.csv'))

    stocks_df = (spark.read.format("json")
        .option("multiline","true")
        .load('data/stocks_dataset.json'))
    stocks_df = stocks_df.withColumn("Date", stocks_df["Date"].cast(T.DateType()))
    stocks_df = stocks_df.withColumn("Interest_Rate", stocks_df["Interest_Rate"].cast(T.DoubleType()))
    stocks_df = stocks_df.withColumn("Stock_Price", stocks_df["Stock_Price"].cast(T.FloatType()))

    return clients_df, assets_df, stocks_df


def transform_data(clients_df, assets_df, stocks_df, config):
    """Transform original datasets.

    :param clients_df: Input clients DataFrame.
    :param assets_df: Input assets DataFrame
    :param stocks_df: Input stocks DataFrame
    :param config: Dictionaty with configuration values such as "inflation",
        "depreciation" and "property_revaluation"
    :return: Transformed DataFrame.
    """
    stock_price_today = stocks_df.first()["Stock_Price"]
    
    clients_assets_df = (clients_df
        .join(assets_df, on = clients_df.Id == assets_df.Client_Id)
        .drop(clients_df.Id)
        .withColumnRenamed("Id", "Asset_Id"))
    clients_assets_stocks_df = (clients_assets_df
        .join(stocks_df, on = clients_assets_df.Date_Market_Value == stocks_df.Date)\
        .drop(stocks_df.Date)
        .drop(stocks_df.News)
        .drop(stocks_df.Interest_Rate))

    clients_assets_stocks_df = (clients_assets_stocks_df
        .withColumn("Days_Between_Market_Value_And_Today", datediff(current_date(), col("Date_Market_Value")))
        .withColumn("Market_Value", 
                when(col("Asset") == "Stocks", clients_assets_stocks_df.Stock_Price)
                .otherwise(clients_assets_stocks_df.Market_Value))
        .withColumn('Current_Market_Value',
                when(col("Asset") == "Stocks", 
                        stock_price_today)
                .when(col("Asset") == "Savings", 
                        col("Market_Value") * (1 - lit(config["inflation"]) * col("Days_Between_Market_Value_And_Today")))
                .when(col("Asset") == "Cars",
                        col("Market_Value") * (1 - lit(config["depreciation"]) * col("Days_Between_Market_Value_And_Today")))
                .when(col("Asset") == "Property",
                        col("Market_Value") * (1 + lit(config["property_revaluation"]) * col("Days_Between_Market_Value_And_Today")))
                ))
    
    clients_assets_value_fluctuation = clients_assets_stocks_df.groupBy("Client_Id", "Name", "Surname").sum("Market_Value", "Current_Market_Value").\
            toDF("Client_Id", "Name", "Surname", "Market_Value", "Current_Market_Value"). \
            withColumn("Current_Market_Value", round(col("Current_Market_Value"), 2)). \
            withColumn("Absolute_Assets_Value_Fluctuation", round(col("Current_Market_Value") - col("Market_Value"), 2)). \
            withColumn("Percentage_Assets_Value_Fluctuation", round(col("Absolute_Assets_Value_Fluctuation") / col("Market_Value") *  100, 4))

    return clients_assets_value_fluctuation


def load_data(clients_assets_value_fluctuation):
    """Collect data locally and save as a parquet and a table.

    :param df: transformed DataFrame to save.
    :return: None
    """
    csv = 'clients_assets_value_fluctuation'
    clients_assets_value_fluctuation.write. \
        option("header", "true"). \
        option("delimiter", ","). \
        mode("overwrite"). \
        csv("data/" + csv)

    return None


# entry point for PySpark ETL application
if __name__ == '__main__':
    main()