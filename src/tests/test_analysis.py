import pytest
from nyc_taxi_analysis.utils import get_spark_session
from nyc_taxi_analysis.analysis import NYCTaxiAnalysis

@pytest.fixture(scope="module")
def spark():
    spark_session = get_spark_session("Test App")
    yield spark_session
    spark_session.stop()

@pytest.fixture(scope="module")
def analysis(spark):
    return NYCTaxiAnalysis(spark)

def test_load_data(analysis):
    df = analysis.load_data("data/yellow_tripdata_2021-01.csv")
    assert df is not None
    assert df.count() > 0

def test_preprocess_data(analysis):
    df = analysis.load_data("data/yellow_tripdata_2021-01.csv")
    df = analysis.preprocess_data()
    assert df is not None
    assert df.filter(df["tpep_pickup_datetime"].isNull()).count() == 0
    assert df.filter(df["tpep_dropoff_datetime"].isNull()).count() == 0

def test_add_time_columns(analysis):
    df = analysis.load_data("data/yellow_tripdata_2021-01.csv")
    df = analysis.preprocess_data()
    df = analysis.add_time_columns()
    assert "pickup_year" in df.columns
    assert "pickup_month" in df.columns
    assert "pickup_day" in df.columns
    assert "pickup_hour" in df.columns

def test_analyze_trips_per_day(analysis):
    df = analysis.load_data("data/yellow_tripdata_2021-01.csv")
    df = analysis.preprocess_data()
    df = analysis.add_time_columns()
    df_grouped = analysis.analyze_trips_per_day()
    assert df_grouped is not None
    assert df_grouped.count() > 0

def test_analyze_average_fare(analysis):
    df = analysis.load_data("data/yellow_tripdata_2021-01.csv")
    df = analysis.preprocess_data()
    df = analysis.add_time_columns()
    df_avg_fare = analysis.analyze_average_fare()
    assert df_avg_fare is not None
    assert df_avg_fare.count() > 0
