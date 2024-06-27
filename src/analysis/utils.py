from pyspark.sql import SparkSession

def get_spark_session(app_name):
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.executor.memory", "4g") \
        .config("spark.executor.cores", "2") \
        .config("spark.sql.shuffle.partitions", "200") \
        .getOrCreate()
    return spark
