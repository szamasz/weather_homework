from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from weather_homework.schemas import *
from weather_homework.etl_functions import structurize, toArray
from pathlib import Path

base_dir = str(Path(__file__).parent.parent.absolute())

spark = SparkSession.builder.master("local[*]").appName("load pressure Stockholm").getOrCreate()

df_in_press_text_1753 = spark.read.text(base_dir + "/hdfs/raw/1753_1858_press.txt")
df_in_press_text_1859 = spark.read.text(base_dir + "/hdfs/raw/1859_1861_press.txt")
df_in_press_text_1862 = spark.read.text(base_dir + "/hdfs/raw/1862_1937_press.txt")
df_in_press_text_modern = spark.read.text([
    base_dir + "/hdfs/raw/1938_1960_press.txt",
    base_dir + "/hdfs/raw/1961_2012_press.txt", base_dir + "/hdfs/raw/2013_2017_manual_press.txt"
])
df_in_press_text_modern_auto = spark.read.text(base_dir + "/hdfs/raw/2013_2017_automatic_press.txt")


df_str_press_1753 = structurize(toArray(df_in_press_text_1753), schema_press_1753, schema_add_press_1753)
df_clean_1753 = df_str_press_1753.withColumn("b_morn", F.col("b_morn") * 29.69 * 1.33322). \
    withColumn("b_noon", F.col("b_noon") * 29.69 * 1.33322). \
    withColumn("b_evn", F.col("b_evn") * 29.69 * 1.33322).replace(float('NaN'), None)
df_str_press_1859 = structurize(toArray(df_in_press_text_1859), schema_press_1859, schema_add_press_1859)
df_clean_1859 = df_str_press_1859.withColumn("b_morn", F.col("b_morn") * 2.969 * 1.33322). \
    withColumn("b_noon", F.col("b_noon") * 2.969 * 1.33322).withColumn("b_evn", F.col("b_evn") * 2.969 * 1.33322).\
    withColumn("p_morn_0", F.col("p_morn_0") * 2.969 * 1.33322).\
    withColumn("p_noon_0", F.col("p_noon_0") * 2.969 * 1.33322).\
    withColumn("p_noon_0", F.col("p_noon_0") * 2.969 * 1.33322).\
    replace(float('NaN'), None)
df_str_press_1862 = structurize(toArray(df_in_press_text_1862), schema_press_modern, schema_add_press_modern)
df_clean_1862 = df_str_press_1862.withColumn("b_morn", F.col("b_morn") * 1.33322).\
    withColumn("b_noon", F.col("b_noon") * 1.33322).withColumn("b_evn", F.col("b_evn") * 1.33322).\
    replace(float('NaN'), None)
df_str_press_modern = structurize(toArray(df_in_press_text_modern), schema_press_modern, schema_add_press_modern)
df_clean_modern = df_str_press_modern.replace(float('NaN'), None)
df_str_press_modern_auto = structurize(
    toArray(df_in_press_text_modern_auto),
    schema_press_modern,
    schema_add_press_modern
)
df_clean_modern_auto = df_str_press_modern_auto.replace(float('NaN'), None).withColumn("method", F.lit("automatic"))

df_press_comb = df_clean_1753.union(df_clean_1859).unionByName(df_clean_1862).unionByName(df_clean_modern).\
    unionByName(df_clean_modern_auto)

df_press_comb.createOrReplaceGlobalTempView("pressure")
df_press_comb.write.partitionBy('year').parquet(base_dir + "/hdfs/clean/pressure", mode="overwrite")
