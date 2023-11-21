import os
import boto3
from pyspark.sql import SparkSession
from transformations import group_by_store,join_with_stores,filter_temperature_above_threshold,calculate_percentage_unemployment


def create_spark_session(aws_access_key_id,aws_secret_access_key):
    '''
    create and return spark session with s3 configuration
    '''
    spark= SparkSession.builder.appName("TAIYO.AI").getOrCreate()

    spark._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark._jsc.hadoopConfiguration().set("com.amazonaws.services.s3.enableV4", "true")
    spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", aws_access_key_id)
    spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", aws_secret_access_key)
    spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.amazonaws.com")
    spark._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "true")
    spark._jsc.hadoopConfiguration().set("com.amazonaws.services.s3a.enableV4", "true")

    spark.conf.set("spark.driver.extraClassPath", "jars/hadoop-aws-3.3.1.jar")

    return spark

#reading the data
def read_data(spark,path,file_format):
    return spark.read.format(file_format).option("header","true").option("inferSchema","true").load(path)

#write the data

def write_data(df,path, file_format,compression =None):
    if compression:
        df.write.format(file_format).mode("overwrite").option("compression",compression).save(path)
    else:
        df.write.format(file_format).mode("overwrite").save(path)

def main(aws_access_key_id,aws_secret_access_key):


    #initialize spark session
    spark = create_spark_session(aws_access_key_id,aws_secret_access_key)

    #Read Data
    features_df = read_data(spark, "s3://ram-task/features.csv","csv")
    stores_df = read_data(spark, "s3://ram-task/stores.csv","csv")
    
    #Transformation -1 : group by store and calculate sums
    grouped_df = group_by_store(features_df)

    #Transformation -2 : join the DataFrames with Store Column
    joined_df = join_with_stores(grouped_df,stores_df)

    #Transformation -3 : Filter rows where temperature is above 50
    filtered_df = filter_temperature_above_threshold(joined_df,50)

    #Transformation -4: Calculate percentage of Unemployement
    percentage_df = calculate_percentage_unemployment(filtered_df)

    #write results in different formats
    write_data(percentage_df, "s3://ram-task/ouput_parquet","parquet")
    write_data(percentage_df, "s3://ram-task/ouput_json","json")
    write_data(percentage_df, "s3://ram-task/ouput_csv","csv")
    write_data(percentage_df, "s3://ram-task/ouput_gzip","parquet",compression="gzip")

if __name__ == "__main__":
    #Read AWS Credentials from env vairables
    aws_access_key_id = "AWS ACESS KEY ID"
    aws_secret_access_key = "AWS SECRET ACCESS KEY"

    main(aws_access_key_id,aws_secret_access_key)