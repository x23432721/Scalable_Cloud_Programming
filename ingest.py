# ingest.py
import json
import boto3
from datasets import load_dataset

REGION      = "eu-west-1"
STREAM_NAME = "amazon-reviews"

# Initialize Kinesis client
kinesis = boto3.client("kinesis", region_name=REGION)

# Load 1000-sample
ds = load_dataset("amazon_polarity", split="train[:1000]")

for i, rec in enumerate(ds):
    payload = {
        "review_id":   str(i),
        "review_text": rec["content"],
        "label":       rec["label"]
    }
    kinesis.put_record(
        StreamName=STREAM_NAME,
        Data=json.dumps(payload),
        PartitionKey=str(i)
    )
    if (i + 1) % 100 == 0:
        print(f"Sent {i+1} records")

print("Done sending to Kinesis.")

