from re import A
from pyspark.sql import SparkSession
from weather_homework.etl_functions import toArray,max_size,structurize
from pyspark.sql.types import IntegerType,FloatType,StructType,StructField
import pytest
import pyspark.sql.functions as F


spark = SparkSession.builder.master("local[*]").appName("tests").getOrCreate()

def test_toArray_ok():
    df=spark.createDataFrame([("1990 1 12 3 2.3 13.3 test",)],["value"])
    df_out=toArray(df)
    df_test=spark.createDataFrame([("1990 1 12 3 2.3 13.3 test",["1990","1","12","3","2.3","13.3","test"],)],["value","array"])
    assert df_test.collect() == df_out.collect()

def test_toArray_single():
    df=spark.createDataFrame([("1990",)],["value"])
    df_out=toArray(df)
    df_test=spark.createDataFrame([("1990",["1990"],)],["value","array"])
    assert df_test.collect() == df_out.collect()

def test_toArray_empty_string():
    df=spark.createDataFrame([("",)],["value"])
    df_out=toArray(df)
    df_test=spark.createDataFrame([("",[""],)],["value","array"])
    assert df_test.collect() == df_out.collect()

def test_toArray_empty_string():
    df=spark.createDataFrame([("",)],["value"])
    df_out=toArray(df)
    df_test=spark.createDataFrame([("",[""],)],["value","array"])
    assert df_test.collect() == df_out.collect()

def test_maxsize_equal():
    df=spark.createDataFrame([(["1990","1","test"],),(["323","3","test2"],)],["array"])
    assert max_size(df) == 3

def test_maxsize_notequal():
    df=spark.createDataFrame([(["1990","1","test"],),(["323"],)],["array"])
    assert max_size(df) == 3


def test_structurize():
    df=spark.createDataFrame([(["1523", "12", "12", "3.2"],)],["array"])
    schema_test=StructType([
    StructField("year", IntegerType(), False),
    StructField("month", IntegerType(), False),
    StructField("day", IntegerType(), False),
    StructField("b_morn", FloatType(), False)
    ])
    df_out=structurize(df,schema_test)
    df_test=spark.createDataFrame([(1523, 12, 12, 3.2,"manual")],["year","month","day","b_morn","method"])
    df_test_dict=df_test.collect()[0].asDict()
    df_out_dict=df_out.collect()[0].asDict()
    assert df_test_dict['year'] == df_out_dict['year']
    assert df_test_dict['month'] == df_out_dict['month']
    assert df_test_dict['day'] == df_out_dict['day']
    assert df_test_dict['b_morn'] == pytest.approx(df_out_dict['b_morn'])
    assert df_test_dict['method'] == df_out_dict['method']

def test_structurize_additional_fields():
    df=spark.createDataFrame([(["1523", "12", "12", "3.2"],)],["array"])
    schema_test=StructType([
    StructField("year", IntegerType(), False),
    StructField("month", IntegerType(), False),
    StructField("day", IntegerType(), False),
    StructField("b_morn", FloatType(), False)
    ])
    schema_test_add=StructType([
    StructField("tmin", FloatType(), False)
    ])
    df_out=structurize(df,schema_test,schema_test_add)
    df_test=spark.createDataFrame([(1523, 12, 12, 3.2,"manual")],["year","month","day","b_morn","method"]).withColumn("tmin",F.lit(None))
    df_test_dict=df_test.collect()[0].asDict()
    df_out_dict=df_out.collect()[0].asDict()
    assert df_test_dict['year'] == df_out_dict['year']
    assert df_test_dict['month'] == df_out_dict['month']
    assert df_test_dict['day'] == df_out_dict['day']
    assert df_test_dict['b_morn'] == pytest.approx(df_out_dict['b_morn'])
    assert df_test_dict['method'] == df_out_dict['method']
    assert df_out_dict['tmin']==None