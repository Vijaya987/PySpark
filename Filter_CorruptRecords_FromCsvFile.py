# Import required packages
from pyspark.sql.functions import col 
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Define schema for the data
schema = StructType([ StructField("Id", IntegerType()), 
                     StructField("course_name", StringType()), 
                     StructField("dept_name", StringType()),
                    StructField("_corrupt_record", StringType(), True)])
       
    
# Read the csv file that containes corrupt/bad records using spark dataframe
employee_df = spark.read.option("mode", "PERMISSIVE").schema(schema)\
    .option("header", True)\
    .option("columnNameOfCorruptRecord", "_corrupt_record")\
    .csv("dbfs:/FileStore/mt4037/coursecsv.csv").cache()

# Print the data in csv file with corrupted records in a separate column
employee_df.show()

# Extract corrupt records column from the dataframe
corrupt_records=employee_df.select(col('_corrupt_record'))
corrupt_records.show()
                                   
# Store the corrupt/bad records in storage container in parquet format 
corrupt_records.write.mode("overwrite").format("parquet").option("header","true").save("abfss://mt4037@trainingnew.dfs.core.windows.net/sample/corrupt_records.parquet")
