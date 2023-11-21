from pyspark.sql import functions as F

# Group the DataFrame by "Store" and calculating the sum of "Temperature" and "Unemployment"
def group_by_store(df):
    return df.groupBy("Store").agg(
        F.sum("Temperature").alias("sum_temperature"),
        F.sum("Unemployment").alias("sum_unemployment")
    )

#joining two DataFrames on the "Store" column
def join_with_stores(df1,df2):
    return df1.join(df2,"Store","inner")

#Filter rwos where Temperature is above a threshold
def filter_temperature_above_threshold(df, threshold_value):
    return df.filter(F.col("Temperature") > threshold_value)

#calculating the percentage of "Unemployment" based on total sum
def calculate_percentage_unemployment(df):
    total_unemployment = df.agg(F.sum("sum_unemployment")).collect()[0][0]
    return df.withColumn("Percentage_Unemployment",(F.col("sum_unemployment")/ total_unemployment) * 100)
