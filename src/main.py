from analysis.utils import get_spark_session
from analysis.analysis import NYCTaxiAnalysis

if __name__ == "__main__":
    spark = get_spark_session("NYC Taxi Analysis")

    analysis = NYCTaxiAnalysis(spark)
    df = analysis.load_data("data/2021_Yellow_Taxi_Trip_Data.csv")
    date_format = "MM/dd/yyyy hh:mm:ss a"
    df = analysis.preprocess_data(date_format)
    df = analysis.add_time_columns()
    #analysis.persist_data()

    print("Sample Data:")
    analysis.show_sample_data(10)

    print("Trips per Day:")
    trips_per_day = analysis.analyze_trips_per_day()
    trips_per_day.show(10)

    print("Average Fare per Day:")
    avg_fare_per_day = analysis.analyze_average_fare()
    avg_fare_per_day.show(10)

    analysis.stop_spark()
