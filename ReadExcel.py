import pandas as pd
import base64

# Import spark session
from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id

# Create spark context
spark = SparkSession.builder.master("local[1]").appName("Excel").getOrCreate()

# Read excel file with multiple sheets
pdf = pd.read_excel('C:/Work_area/ScalaSbt/UserStory7/SourceFile/Spark_US7.xlsx', sheet_name=None)
print(pdf)

# JDBC connection details
jdbcDriver = "org.postgresql.Driver"
jdbcHostname = "w3.training5.modak.com"
jdbcPort = "5432"
jdbcDatabase = "training"
url = "jdbc:postgresql://{0}:{1}/{2}".format(jdbcHostname, jdbcPort, jdbcDatabase)

# Decode the username (encoded using base64 encoder)
decodedUser = base64.b64decode(b'bXQ0MDM3')
username = decodedUser.decode('ascii')

# Decode the password (encoded using base64 encoder)
decodedPassword = base64.b64decode(b'dmlqYXlh')
password = decodedPassword.decode('ascii')

for key in pdf.keys():
    print(key)
    df_new = spark.createDataFrame(pdf[key])
    print(df_new)
    table_name = 'mt4037_' + key
    print(table_name)
    df_new.show()
    df_new.write.format("jdbc").mode("overwrite").option("url", url) \
        .option("driver", jdbcDriver).option("dbtable", table_name) \
        .option("user", username).option("password", password).save()


