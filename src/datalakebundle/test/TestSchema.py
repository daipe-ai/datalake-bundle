import pyspark.sql.types as T

def getSchema():
    return T.StructType([
        T.StructField('name', T.StringType(), True),
        T.StructField('createTime', T.TimestampType(), True),
    ])
