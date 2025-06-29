import re
import time
import logging
import boto3
import botocore
import nltk
from multiprocessing import Pool
from textblob import TextBlob

# Ensure sentence tokenizer is available
nltk.download('punkt', quiet=True)

# — CONFIGURATION —
IN_BUCKET   = "text-processing-intermediate-bucket-sachin"
OUT_BUCKET  = "text-processing-output-bucket-sachin"
PREFIX      = "chunks/"
MAX_RETRIES = 3
POOL_SIZE   = 4

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

s3 = boto3.client('s3')

def fetch_text(key):
    """Download and return the full text of one S3 object, retrying on streaming errors."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            obj = s3.get_object(Bucket=IN_BUCKET, Key=key)
            return obj['Body'].read().decode('utf-8')
        except botocore.exceptions.ResponseStreamingError as e:
            logging.warning(f"Stream error for {key} (attempt {attempt}/{MAX_RETRIES}): {e}")
            time.sleep(1)
    # last attempt without catching to propagate error
    obj = s3.get_object(Bucket=IN_BUCKET, Key=key)
    return obj['Body'].read().decode('utf-8')

def process_chunk(key):
    try:
        text = fetch_text(key)
    except Exception as e:
        logging.error(f"Failed to fetch {key}: {e}")
        return {}, 0.0

    # Word count
    words = re.findall(r'\w+', text.lower())
    wc    = {}
    for w in words:
        if len(w) > 2:
            wc[w] = wc.get(w, 0) + 1

    # Sentiment
    blob = TextBlob(text)
    pols = [s.sentiment.polarity for s in blob.sentences]
    avg_pol = sum(pols) / len(pols) if pols else 0.0

    return wc, avg_pol

def combine(results):
    total_wc = {}
    pols     = []
    for wc, pol in results:
        pols.append(pol)
        for w, c in wc.items():
            total_wc[w] = total_wc.get(w, 0) + c
    avg_pol = sum(pols) / len(pols) if pols else 0.0
    return total_wc, avg_pol

if __name__ == "__main__":
    logging.info("Listing chunk keys from S3…")
    resp = s3.list_objects_v2(Bucket=IN_BUCKET, Prefix=PREFIX)
    contents = resp.get('Contents') or []
    keys = [o['Key'] for o in contents]

    if not keys:
        logging.error("No chunks found in intermediate bucket. Exiting.")
        exit(1)

    logging.info(f"Found {len(keys)} chunks. Starting processing with {POOL_SIZE} workers…")

    with Pool(processes=POOL_SIZE) as pool:
        results = pool.map(process_chunk, keys)

    logging.info("Combining results…")
    word_counts, avg_sentiment = combine(results)

    logging.info("Top 5 words: %s", sorted(word_counts.items(), key=lambda x: -x[1])[:5])
    logging.info("Average sentiment: %.3f", avg_sentiment)

    # Prepare CSV of top 50
    top50 = sorted(word_counts.items(), key=lambda x: -x[1])[:50]
    csv   = "word,count\n" + "\n".join(f"{w},{c}" for w,c in top50)

    logging.info("Saving results back to S3…")
    s3.put_object(Bucket=OUT_BUCKET, Key="results/top50.csv", Body=csv.encode('utf-8'))
    s3.put_object(Bucket=OUT_BUCKET, Key="results/average_sentiment.txt",
                  Body=str(avg_sentiment).encode('utf-8'))

    logging.info("✅ MapReduce processing complete")


