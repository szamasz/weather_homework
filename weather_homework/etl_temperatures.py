from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from weather_homework.schemas import *
from weather_homework.etl_functions import *


from pathlib import Path

base_dir= str(Path( __file__ ).parent.parent.absolute())

spark = SparkSession.builder.master("local[*]").appName("load temperatures Stockholm").getOrCreate()

df_in_temp_text_manual=spark.read.text([str(base_dir)+"/hdfs/raw/1756_1858_temp.txt",base_dir+"/hdfs/raw/1859_1960_temp.txt",base_dir+"/hdfs/raw/1961_2012_temp.txt",
base_dir+"/hdfs/raw/2013_2017_manual_temp.txt"
])
df_in_temp_text_auto=spark.read.text(base_dir+"/hdfs/raw/2013_2017_automatic_temp.txt")

df_str_temp_manual=structurize(toArray(df_in_temp_text_manual),schema_temp)
df_clean_temp_manual=df_str_temp_manual.replace(float('NaN'),None)
df_str_temp_auto=structurize(toArray(df_in_temp_text_auto),schema_temp)
df_clean_temp_auto=df_str_temp_auto.replace(float('NaN'),None).withColumn("method",F.lit("automatic"))

df_temp_comb=df_clean_temp_manual.unionByName(df_clean_temp_auto)

df_temp_comb.createOrReplaceGlobalTempView("temperatures")
df_temp_comb.write.partitionBy('year').parquet(base_dir+"/hdfs/clean/temperatures",mode="overwrite")