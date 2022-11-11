import pandas
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]").appName("Test").getOrCreate()

pdf = pandas.read_excel('C:/Users/mt4037/Downloads/sample.xlsx', sheet_name='Expenses')

pdf

from pyspark.sql.types import StructType, StructField, DoubleType, StringType

schema = StructType([StructField("Item", StringType(), True), StructField("Amount", DoubleType(), True),  StructField("April", DoubleType(), True)])
spark.conf.set("spark.sql.execution.arrow.enabled","true")

df = spark.createDataFrame(pdf,schema=schema)

df.printSchema

df.show()

df.select("Item","Amount","April").write.format("jdbc").mode("overwrite")\
     .option("url", "jdbc:postgresql://w3.training5.modak.com:5432/training") \
     .option("driver", "org.postgresql.Driver").option("dbtable", "sample_mt4037") \
     .option("user", "mt4037").option("password", "vijaya").save()

