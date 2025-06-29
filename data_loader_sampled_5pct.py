# data_loader_sampled_5pct.py

from datasets import load_dataset                     # Hugging Face Datasets API
import boto3

# Configure AWS
s3 = boto3.client('s3')
INPUT_BUCKET = 'text-processing-input-bucket-sachin'

# 1) Load the full AG News train split
dataset = load_dataset("ag_news", split="train")

# 2) Shuffle and select 5%
total = len(dataset)
sample_size = int(0.02 * total)
sampled = dataset.shuffle(seed=42).select(range(sample_size))

print(f"Total records: {total}, sampling 5% → {sample_size} records")

# 3) Upload each sampled article to S3
for idx, example in enumerate(sampled):
    text = example.get('text', "")
    if not text:
        continue

    key = f"articles/agnews_5pct_{idx:05d}.txt"
    s3.put_object(
        Bucket=INPUT_BUCKET,
        Key=key,
        Body=text.encode('utf-8')
    )
    if idx % 500 == 0:
        print(f"Uploaded {idx} → s3://{INPUT_BUCKET}/{key}")

print("Done uploading 5% sampled AG News articles.")

