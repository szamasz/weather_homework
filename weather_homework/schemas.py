from pyspark.sql.types import IntegerType,FloatType,NullType,StructType,StructField


schema_press_1753=StructType([
    StructField("year", IntegerType(), False),
    StructField("month", IntegerType(), False),
    StructField("day", IntegerType(), False),
    StructField("b_morn", FloatType(), False),
    StructField("t_morn", FloatType(), False),
    StructField("b_noon", FloatType(), False),
    StructField("t_noon", FloatType(), False),
    StructField("b_evn", FloatType(), False),
    StructField("t_evn", FloatType(), False)
])
schema_add_press_1753=StructType([
  StructField("p_morn_0", FloatType(), False),
  StructField("p_noon_0", FloatType(), False),
  StructField("p_evn_0", FloatType(), False),
])
schema_press_1859=StructType([
    StructField("year", IntegerType(), False),
    StructField("month", IntegerType(), False),
    StructField("day", IntegerType(), False),
    StructField("b_morn", FloatType(), False),
    StructField("t_morn", FloatType(), False),
    StructField("p_morn_0", FloatType(), False),
    StructField("b_noon", FloatType(), False),
    StructField("t_noon", FloatType(), False),
    StructField("p_noon_0", FloatType(), False),
    StructField("b_evn", FloatType(), False),
    StructField("t_evn", FloatType(), False),
    StructField("p_evn_0", FloatType(), False)
])
schema_add_press_1859=StructType([])
schema_press_modern=StructType([
    StructField("year", IntegerType(), False),
    StructField("month", IntegerType(), False),
    StructField("day", IntegerType(), False),
    StructField("b_morn", FloatType(), False),
    StructField("b_noon", FloatType(), False),    
    StructField("b_evn", FloatType(), False)    
])
schema_add_press_modern=StructType([
  StructField("t_morn", FloatType(), False),
  StructField("t_noon", FloatType(), False),
  StructField("t_evn", FloatType(), False),
  StructField("p_morn_0", FloatType(), False),
  StructField("p_noon_0", FloatType(), False),
  StructField("p_evn_0", FloatType(), False),
])

schema_temp=StructType([
    StructField("year", IntegerType(), False),
    StructField("month", IntegerType(), False),
    StructField("day", IntegerType(), False),
    StructField("t_morn", FloatType(), False),
    StructField("t_noon", FloatType(), False),
    StructField("t_evn", FloatType(), False),
    StructField("tmax", FloatType(), False),
    StructField("tmin", FloatType(), False),
    StructField("tmean", FloatType(), False)
])