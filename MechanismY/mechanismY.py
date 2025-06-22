from pyspark.sql.functions import col, count, avg, percentile_approx, when, current_timestamp, lit
from datetime import datetime
import gdown
import os
import psycopg2
import uuid
file_id = "1abe9EkM_uf2F2hjEkbhMBG9Mf2dFE4Wo"
gdown.download(f"https://drive.google.com/uc?id={file_id}", "transactions.csv", quiet=False)


customer_weights_df = spark.read.option("header", True).csv("CustomerImportance.csv")
customer_weights_df.show()

#postgres connection setup
def write_to_postgres(df, table):
    df.write \
      .format("jdbc") \
      .option("url", "jdbc:postgresql://localhost:5432/pyspark_stream") \
      .option("dbtable", "pyspark_stream_table") \
      .option("user", "root") \
      .option("password", "root") \
      .mode("append") \
      .save()

def detect_patterns(trans_chunk_df):
    detections = []

    df = trans_chunk_df.join(customer_weights_df, on=["customerName", "transactionType"], how="left")

    # Pattern 1
    merchant_txn_counts = df.groupBy("merchantId", "customerName") \
                            .agg(count("*").alias("txn_count"), avg("weightage").alias("avg_weight"))
    
    total_txns_per_merchant = df.groupBy("merchantId").agg(count("*").alias("total_txns"))

    top_customers = merchant_txn_counts.join(total_txns_per_merchant, "merchantId") \
        .filter("total_txns > 50000")

    top_10_pctile_txn = df.groupBy("merchantId").agg(percentile_approx("txn_count", 0.9).alias("top_10"))
    bottom_10_pctile_weight = df.groupBy("merchantId").agg(percentile_approx("weightage", 0.1).alias("bottom_10_weight"))

    pat1_df = top_customers.join(top_10_pctile_txn, "merchantId") \
                           .join(bottom_10_pctile_weight, "merchantId") \
                           .filter((col("txn_count") >= col("top_10")) & (col("avg_weight") <= col("bottom_10_weight"))) \
                           .selectExpr("merchantId", "customerName", "'PatId1' as patternId", "'UPGRADE' as actionType")

    detections.append(pat1_df)

    # Pattern 2
    pat2_df = df.groupBy("merchantId", "customerName") \
        .agg(avg("amount").alias("avg_amt"), count("*").alias("cnt")) \
        .filter("avg_amt < 23 and cnt >= 80") \
        .selectExpr("merchantId", "customerName", "'PatId2' as patternId", "'CHILD' as actionType")

    detections.append(pat2_df)

    #Pattern 3
    gender_df = df.select("merchantId", "customerName", "gender").distinct()
    pat3_df = gender_df.groupBy("merchantId") \
        .agg(
            count(when(col("gender") == "Female", True)).alias("female"),
            count(when(col("gender") == "Male", True)).alias("male")
        ).filter("female > 100 and female < male") \
        .selectExpr("merchantId", "'' as customerName", "'PatId3' as patternId", "'DEI-NEEDED' as actionType")

    detections.append(pat3_df)

    # Combine all detections
    final_detections = detections[0]
    for d in detections[1:]:
        final_detections = final_detections.unionByName(d)

    # Add timestamps
    current_time = datetime.now().astimezone().strftime('%Y-%m-%d %H:%M:%S')
    final_detections = final_detections.withColumn("YStartTime", lit(current_time)) \
                                       .withColumn("detectionTime", current_timestamp())

    return final_detections

# Mechanism Y Loop
processed_chunks = set()

while True:
    chunk_files = dbutils.fs.ls("s3://pyspark-stream/Input/")
    new_files = [f.path for f in chunk_files if f.path not in processed_chunks]

    for chunk_path in new_files:
        df_chunk = spark.read.option("header", True).csv(chunk_path)
        detections_df = detect_patterns(df_chunk)

        # Write detections in batches of 50
        if detections_df.count() > 0:
            detections_df = detections_df.limit(50)
            unique_id = str(uuid.uuid4())
            detections_df.write.mode("overwrite").json(f"s3://pyspark-stream/Output/detect_{unique_id}.json")

        processed_chunks.add(chunk_path)

    time.sleep(1)
