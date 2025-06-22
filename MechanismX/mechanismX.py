from pyspark.sql.functions import lit
import gdown
import time
import requests

# Step 1: Read full transactions.csv from GDrive

file_id = "1AGXVlDhbMbhoGXDJG0IThnqz86Qy3hqb"
gdown.download(f"https://drive.google.com/uc?id={file_id}", "transactions.csv", quiet=False)


df = spark.read.option("header", True).csv("transactions.csv")
df.show()


# Step 2: Chunking
chunk_size = 10000
total_rows = df.count()
num_chunks = (total_rows + chunk_size - 1) // chunk_size

for i in range(num_chunks):
    chunk = df.limit(chunk_size).subtract(df.limit(i * chunk_size))
    timestamp = int(time.time())
    chunk.write.mode("overwrite").option("header", True).csv(f"s3://pyspark-stream/Input/chunk_{timestamp}")
    time.sleep(1)
