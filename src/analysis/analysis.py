from pyspark.sql.functions import col, year, month, dayofmonth, hour, avg, to_timestamp
from pyspark import StorageLevel

class NYCTaxiAnalysis:
    def __init__(self, spark_session):
        self.spark = spark_session

    def load_data(self, file_path):
        self.df = self.spark.read.csv(file_path, header=True, inferSchema=True)
        return self.df

    def preprocess_data(self,date_format):
        self.df = self.df.withColumn("tpep_pickup_datetime", to_timestamp("tpep_pickup_datetime", date_format)) \
                         .withColumn("tpep_dropoff_datetime", to_timestamp("tpep_dropoff_datetime", date_format))
        self.df = self.df.dropna()
        return self.df

    def add_time_columns(self):
        self.df = self.df.withColumn("pickup_year", year("tpep_pickup_datetime")) \
                         .withColumn("pickup_month", month("tpep_pickup_datetime")) \
                         .withColumn("pickup_day", dayofmonth("tpep_pickup_datetime")) \
                         .withColumn("pickup_hour", hour("tpep_pickup_datetime"))
        return self.df

    def analyze_trips_per_day(self):
        # Reparticionar el DataFrame antes de la operación de agrupamiento
        self.df = self.df.repartition(200, "pickup_day")
        df_grouped = self.df.groupBy("pickup_year", "pickup_month", "pickup_day").count()
        return df_grouped

    def analyze_average_fare(self):
        df_avg_fare = self.df.groupBy("pickup_year", "pickup_month", "pickup_day") \
                             .agg(avg("fare_amount").alias("avg_fare"))
        return df_avg_fare

    def persist_data(self):
        self.df.persist(StorageLevel.MEMORY_AND_DISK)

    def show_sample_data(self, num_rows):
        self.df.show(num_rows)

    def stop_spark(self):
        self.spark.stop()
