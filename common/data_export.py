import os

import boto3
from botocore import UNSIGNED
from botocore.client import Config

BUCKET_NAME = "d2b-internal-assessment-bucket"
PREFIX = "orders_data/"
FILE_LIST = ["orders.csv", "reviews.csv", "shipment_deliveries.csv"]
DOWNLOAD_PATH = "../dbt-dags/patrojun6040/data"
SE_REGION = "eu-central-1"


def download_from_s3(bucket_name: str,
                     directory_prefix: str,
                     s3_keys: list[str],
                     download_path: str):
    s3 = boto3.client('s3',
                      region_name=SE_REGION,
                      config=Config(signature_version=UNSIGNED))
    try:
        response = s3.list_objects(Bucket=bucket_name, Prefix=directory_prefix)
        if 'Contents' in response:
            for obj in response['Contents']:
                object_key = obj['Key']
                key = object_key.split("/")[-1]
                if key in s3_keys:
                    download_file_location = f"{download_path}/{object_key}"
                    print(f"downloading {key} in {download_file_location}.")
                    os.makedirs(os.path.dirname(download_file_location), exist_ok=True)
                    s3.download_file(bucket_name, object_key, download_file_location)
    except Exception as e:
        print(f"Error while exporting data from s3: {e}")


if __name__ == '__main__':
    download_from_s3(BUCKET_NAME, PREFIX, FILE_LIST, DOWNLOAD_PATH)
