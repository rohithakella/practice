import boto3
import requests
from botocore.client import Config
from tqdm import tqdm

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

S3_ENDPOINT = "http://localhost:9000"
S3_ACCESS_KEY = "localkey"
S3_SECRET_KEY = "localsecret"
RAW_BUCKET = "datasets"
OUTPUT_BUCKET = "outputs"


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        config=Config(signature_version="s3v4"),
    )


def ensure_buckets(s3):
    existing = {b["Name"] for b in s3.list_buckets()["Buckets"]}
    for bucket in (RAW_BUCKET, OUTPUT_BUCKET):
        if bucket not in existing:
            s3.create_bucket(Bucket=bucket)
            print(f"Created bucket: {bucket}")


def key_exists(s3, bucket: str, key: str) -> bool:
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except Exception:
        return False


def stream_to_s3(s3, url: str, bucket: str, s3_key: str) -> None:
    if key_exists(s3, bucket, s3_key):
        print(f"Already exists, skipping: {s3_key}")
        return

    print(f"Fetching: {s3_key}")
    response = requests.get(url, stream=True)
    response.raise_for_status()

    total = int(response.headers.get("content-length", 0))

    with tqdm(total=total, unit="B", unit_scale=True, desc=s3_key.split("/")[-1]) as bar:
        s3.upload_fileobj(
            Fileobj=_StreamingBody(response, bar),
            Bucket=bucket,
            Key=s3_key,
        )


class _StreamingBody:
    """Wraps a streaming HTTP response so boto3 can read it in chunks."""

    def __init__(self, response, bar):
        self._response = response
        self._bar = bar
        self._iter = response.iter_content(chunk_size=8 * 1024 * 1024)
        self._buf = b""

    def read(self, size=-1):
        try:
            while size < 0 or len(self._buf) < size:
                chunk = next(self._iter)
                self._bar.update(len(chunk))
                self._buf += chunk
        except StopIteration:
            pass
        if size < 0:
            data, self._buf = self._buf, b""
        else:
            data, self._buf = self._buf[:size], self._buf[size:]
        return data


def download_nyc_taxi(year: int = 2023, months: list[int] | None = None) -> None:
    if months is None:
        months = list(range(1, 13))

    s3 = get_s3_client()
    ensure_buckets(s3)

    for month in months:
        filename = f"yellow_tripdata_{year}-{month:02d}.parquet"
        url = f"{BASE_URL}/{filename}"
        s3_key = f"nyc_taxi/{year}/{filename}"
        stream_to_s3(s3, url, RAW_BUCKET, s3_key)

    print(f"\nRaw data  → s3://{RAW_BUCKET}/nyc_taxi/{year}/")
    print(f"Outputs   → s3://{OUTPUT_BUCKET}/")

if __name__ == "__main__":
    download_nyc_taxi(year=2023, months=list(range(1, 13)))
