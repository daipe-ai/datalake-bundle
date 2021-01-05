import pyspark.sql.types as t

def getSchema():
    return t.StructType([
        t.StructField('name', t.StringType(), True),
        t.StructField('createTime', t.TimestampType(), True),
    ])
