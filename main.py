import boto3
from botocore import UNSIGNED
from botocore.client import Config

if __name__ == '__main__':
    bucket_name = 'd2b-internal-assessment-bucket'
    directory_prefix = 'orders_data/'

    # Create an S3 client
    s3 = boto3.client('s3',
                      region_name="eu-central-1",
                      config=Config(signature_version=UNSIGNED))
    try:
        # List objects in the S3 bucket under the specified directory prefix
        response = s3.list_objects(Bucket=bucket_name, Prefix=directory_prefix)

        # Iterate through the objects and print their names
        if 'Contents' in response:
            for obj in response['Contents']:
                # Retrieve the object name
                object_key = obj['Key']
                print(f"Object Key: {object_key}")

                # Download the object if needed
                # s3.download_file(bucket_name, object_key, f"local_path/{object_key}")

    except Exception as e:
        print(f"Error accessing the S3 bucket: {e}")