import pytest
from analysis.utils import get_spark_session

def test_get_spark_session():
    spark = get_spark_session("Test App")
    assert spark is not None
    assert spark.sparkContext.appName == "Test App"
    spark.stop()
