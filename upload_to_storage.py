import os
from google.cloud import storage

client = storage.Client.from_service_account_json(
    json_credentials_path='/Users/rahul/Downloads/gcp-assignment-322710-6f7887027a3a.json')

bucket_name = "demo1_buck"


# creating a new bucket
def create_bucket():
    try:
        print("creating bucket")

        bucket = client.bucket(bucket_name)
        bucket.storage_class = "COLDLINE"
        new_bucket = client.create_bucket(bucket, location="us")

        # print(vars(new_bucket))


    except Exception as err:
        print(err.args)
        raise Exception("Creating Bucket Failed")


def list_buckets():
    try:
        bucket = list(client.list_buckets())
        print(bucket)
    except Exception as err:
        print(err.args)

        raise Exception('listing bucket failed')


def upload_to_bucket(blob_name, file_path):
    '''
    Upload file to a bucket
    : blob_name  (str) - object name
    : file_path (str)
    '''

    try:

        bucket = client.get_bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.upload_from_filename(file_path)

        print(bucket)

        print("Uploading Complete")



    except Exception as err:
        print(err.args)
        raise Exception('Uploading files failed')


def solution():
    try:

        create_bucket();

        list_buckets();

        upload_to_bucket("Customers.csv", "/Users/rahul/Downloads/Customers.csv");
        upload_to_bucket("Orders.csv", "/Users/rahul/Downloads/orders.csv")

    except Exception as err:
        print(err.args)


if __name__ == '__main__':
    solution()
