# import pyspark.sql.functions as pf
df = spark.table('hive_metastore.default.dataset_1_5')
display(df)


# print(df.rdd.getNumPartitions())
# df = df.repartition(4)
# print(df.rdd.getNumPartitions())
# df.groupBy(pf.spark_partition_id()).count().show()
# df.show()


# Write to csv
df.write.mode("overwrite").option("header","true").format("csv").save("dbfs:/FileStore/shared_uploads/vijaya.challagundla@modak.com/dataset.csv")


# Read csv files
csv_df = spark.read.format("csv").option("inferScehma","true").option("header", "true").load("dbfs:/FileStore/shared_uploads/vijaya.challagundla@modak.com/dataset.csv")
csv_df.show()


# Convert csv file into tsv file
csv_df.write.mode("overwrite").format("csv").option("header","true").option("sep","\t").save("dbfs:/FileStore/shared_uploads/vijaya.challagundla@modak.com/dataset.tsv")
tsv_df = spark.read.format("csv").option("header", "true").option("sep","\t").load("dbfs:/FileStore/shared_uploads/vijaya.challagundla@modak.com/dataset.tsv")
tsv_df.printSchema()
tsv_df.show()


# Convert csv file into json files
csv_df.write.mode("overwrite").format("json").option("header","true").save("dbfs:/FileStore/shared_uploads/vijaya.challagundla@modak.com/dataset.json")
json_df = spark.read.option("inferSchema",'true').json("dbfs:/FileStore/shared_uploads/vijaya.challagundla@modak.com/dataset.json")
json_df.show()


# Convert csv files into parquet files
csv_df.write.mode("overwrite").option("header","true").parquet("dbfs:/FileStore/shared_uploads/vijaya.challagundla@modak.com/dataset.parquet")
parquet_df = spark.read.option("inferSchema",'true').parquet("dbfs:/FileStore/shared_uploads/vijaya.challagundla@modak.com/dataset.parquet")
parquet_df.show()


# # Convert csv files into avro format
csv_df.write.mode("overwrite").format("avro").option("header","true").save("dbfs:/FileStore/shared_uploads/vijaya.challagundla@modak.com/dataset.avro")
avro_df = spark.read.format("avro").load("dbfs:/FileStore/shared_uploads/vijaya.challagundla@modak.com/dataset.avro")
avro_df.show()