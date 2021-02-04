import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.types import * # MA
from pyspark.sql import SparkSession # MA
from awsglue.context import GlueContext
from awsglue.job import Job

from datetime import datetime

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

config = {
    "s3_bucket_name" : "s3://ma-weather-data-bucket/",
    "csv_prefix" : "data-process/raw_csv/",
    "parquet_prefix" : "data-process/parquet/",
}

sparkSession = SparkSession(sc)

def read_raw_csv():
    '''
        This function reads the raw csv files
    '''
    try:
        s3_bucket_name = config["s3_bucket_name"]
        csv_path_prefix = config["csv_prefix"]
        csv_files_path = s3_bucket_name + csv_path_prefix
        print("Raw CSV Files Path ... : {}".format(csv_files_path))

        # Read Raw CSV files into a SparkDataframe
        csv_df = sparkSession.read.csv(csv_files_path, inferSchema=True, header=True)

        # Check is the dataframe empty
        if csv_df.rdd.isEmpty():
            print("Source data frame is empty ...")
            raise

        return csv_df
    except Exception as e:
        status = 'Failure'
        print("Error occurred - {}".format(e.args[0]))
        raise


def remove_null_rows(dataframe):
    '''
        This function ignores the rows if there is a NULL value in the 3 columns of importance
        for this Test - observationdate, screentemperature, region
    '''
    try:
        # Remove the rows with NULL values in the columns - observationdate, screentemperature, region
        non_null_df = dataframe.where(dataframe["observationdate"].isNotNull() & dataframe["screentemperature"].isNotNull() & dataframe["region"].isNotNull())

        # Check is the dataframe empty
        if non_null_df.rdd.isEmpty():
            print("Processed data frame is empty ...")
            raise

        return non_null_df
    except Exception as e:
        status = 'Failure'
        print("Error occurred - {}".format(e.args[0]))
        raise

def write_to_parquet(dataframe):
    '''
        This function convert the dataframe into parquet format
        and writes to s3 as parquet files
    '''
    try:
        s3_bucket_name = config["s3_bucket_name"]
        parquet_path_prefix = config["parquet_prefix"]
        parquet_files_path = s3_bucket_name + parquet_path_prefix
        parquet_file_name = "weather" + datetime.today().strftime('_%Y%m%d')
        parquet_file = parquet_files_path + parquet_file_name
        print("Parquet Files Path ... : {}".format(parquet_file))

        #### Create a New Dataframe with Type Casting
        
        # List down the fields
        fields = [
                    StructField("forecastsitecode", LongType(), True)
                  , StructField("observationtime", LongType(), True)
                  , StructField("observationdate", StringType(), True)
                  , StructField("winddirection", LongType(), True)
                  , StructField("windspeed", LongType(), True)
                  , StructField("windgust", LongType(), True)
                  , StructField("visibility", LongType(), True)
                  , StructField("screentemperature", FloatType(), True)
                  , StructField("pressure", LongType(), True)
                  , StructField("significantweathercode", LongType(), True)
                  , StructField("sitename", StringType(), True)
                  , StructField("latitude", FloatType(), True)
                  , StructField("longitude", FloatType(), True)
                  , StructField("region", StringType(), True)
                  , StructField("country", StringType(), True)
        ]
        
        # Create a schema as a Structure Type of fields
        write_schema = StructType(fields)

        parquet_df = sparkSession.createDataFrame(
                        dataframe.select
                              (
                                  dataframe["forecastsitecode"].cast(LongType()).alias("forecastsitecode")
                                , dataframe["observationtime"].cast(LongType()).alias("observationtime")
                                , dataframe["observationdate"].cast(StringType()).alias("observationdate")
                                , dataframe["winddirection"].cast(LongType()).alias("winddirection")
                                , dataframe["windspeed"].cast(LongType()).alias("windspeed")
                                , dataframe["windgust"].cast(LongType()).alias("windgust")
                                , dataframe["visibility"].cast(LongType()).alias("visibility")
                                , dataframe["screentemperature"].cast(FloatType()).alias("screentemperature")
                                , dataframe["pressure"].cast(LongType()).alias("pressure")
                                , dataframe["significantweathercode"].cast(LongType()).alias("significantweathercode")
                                , dataframe["sitename"].cast(StringType()).alias("sitename")
                                , dataframe["latitude"].cast(FloatType()).alias("latitude")
                                , dataframe["longitude"].cast(FloatType()).alias("longitude")
                                , dataframe["region"].cast(StringType()).alias("region")
                                , dataframe["country"].cast(StringType()).alias("country")
                              ).rdd,
                        write_schema)

        # Check is the dataframe empty
        if parquet_df.rdd.isEmpty():
            print("Parquet data frame is empty ...")
            raise

        # Write SparkDataframe to Parquet files
        parquet_df.write.format("parquet").option("compression", "gzip").save(parquet_file).mode(SaveMode.Overwrite)

        status = 'Success'

        return status
    except Exception as e:
        status = 'Failure'

        print("Error occurred - {}".format(e.args[0]))
        raise


# Calling the functions to Read CSV files
print("Stage-1 : Extraction - Reading Raw CSV files ...")
csv_df = read_raw_csv()

# Calling the function to Remove NULL Rows
print("\n\nStage-2 : Data Clean-up - Removing NULL rows ...")
nn_df1 = remove_null_rows(csv_df)

# Calling the function to Write to Parquet
print("\n\nStage-3 : Loading - Writing to Parquet ...")
result = write_to_parquet(nn_df1)
print(" Return from  Writing to Parquet ... - {}".format(result))

job.commit()
