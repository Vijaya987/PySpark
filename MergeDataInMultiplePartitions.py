# Import required packages
import os
import pandas as pd
from pyspark.python.pyspark.shell import spark

# List the partitioned files
directory = '/dbfs/FileStore/shared_uploads/vijaya.challagundla@modak.com/dataset.csv'
files = os.listdir(directory)
print(files)


# Method to read each partition from the directory
def read_files(filenames):
    csvFiles = [file for file in filenames if file.endswith(".csv")]
    for i in csvFiles:
        yield pd.read_csv(directory + '/' + i)


# Call the method
csvData = pd.concat(read_files(files))
# Write the merged partitioned data into a csv file
csvData.to_csv("/dbfs/FileStore/mt4037/csvFilesMerged.csv", header=True)
# csvData.count()
# display(csvData)

# Check the data in the merged file
csvDF = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/mt4037/csvFilesMerged.csv")
csvDF.show()
