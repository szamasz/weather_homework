import pyspark.sql.functions as F


def toArray(df):
    """
    Transforms DF with string variable in the field "value" into array splitting the string based on
    multiple whitespaces
    Args:
        df: DataFrame
    Returns:
        DataFrame
    """
    return df.withColumn("array", F.split(F.trim(F.col("value")), " +"))


def max_size(df):
    """
    Returns size of the biggest array in the DataFrame
    Args:
        df: DataFrame
    Returns:
        integer: max size
    """
    return df.select(F.size(F.col("array")).alias("size")).groupBy().agg(F.max("size").alias("maxsize")).\
        select("maxsize").collect()[0]['maxsize']


def structurize(df, schema, schema_add_cols=None):
    """
    Transforms DF's array into columns with types defined in schema. Add the end extra column 'method' is added
    filled with value 'manual'
    Args:
        df: DataFrame
        schema: schema containing types for the schema
        schema_add_cols: in case the array actually doesn't include all possible fields of the provisioned final DF
        they are filled in with nulls
    Returns:
        DataFrame
    """
    array_maxsize = max_size(df)
    df_struct = df.select(
        *[F.col("array")[i].cast(schema[i].dataType).alias(f"{schema[i].name}") for i in range(array_maxsize)]
    ).withColumn("method", F.lit("manual"))
    if schema_add_cols:
        for col in schema_add_cols.fields:
            df_struct = df_struct.withColumn(f"{col.name}", F.lit(None).cast(col.dataType))
    return df_struct
