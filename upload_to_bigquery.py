from typing import Union, Any

from google.cloud import bigquery
from google.oauth2 import service_account

import os
from google.cloud import storage
import pandas as pd
import pandas_gbq
from pandas import Series, DataFrame
from pandas.core.generic import NDFrame
from pandas.io.parsers import TextFileReader

bucket_name = "demo1_buck"


def download_file_from_bucket(blog_name, file_path):
    try:
        client = storage.Client.from_service_account_json(
            json_credentials_path='/Users/rahul/Downloads/gcp-assignment-322710-6f7887027a3a.json')

        bucket = client.get_bucket(bucket_name)
        blob = bucket.blob(blog_name)
        with open(file_path, 'wb') as f:
            client.download_blob_to_file(blob, f)

    except Exception as err:
        print(err.args)
        raise Exception("Downloading file Failed")


def join_files_to_final_csv():
    try:

        try:

            customers_df = pd.read_csv("customersfile.csv")
            orders_df = pd.read_csv('ordersfile.csv')

        except Exception:
            raise Exception("reading files failed")

        # print(customers_df.columns)
        # print(orders_df.columns)

        customers_orders = pd.merge(customers_df, orders_df, how='left', on='CustomerID')

        customers_orders.to_csv("finalresult.csv", index=False)

    # print(customers_orders)

    except Exception as err:
        print(err.args)
        raise Exception("unable to join files")


def create_dataset(project_id):
    try:

        credentials = service_account.Credentials.from_service_account_file(
            '/Users/rahul/Downloads/gcp-assignment-322710-6d632b671846.json')

        cli = bigquery.Client(project=project_id, credentials=credentials)

        dataset_id = "{}.first_dataset".format(project_id)
        dataset = bigquery.Dataset(dataset_id)
        # Send the dataset to the API for creation, with an explicit timeout.
        # Raises google.api_core.exceptions.Conflict if the Dataset already
        # exists within the project.
        dataset = cli.create_dataset(dataset, timeout=30)  # Make an API request.
        print("Created dataset {}.{}".format(cli.project, dataset.dataset_id))



    except Exception as err:

        print(err.args)
        raise Exception("Dataset creation failed")


def create_table(project_id, dataset_id, table_name, schema):
    try:

        credentials = service_account.Credentials.from_service_account_file(
            '/Users/rahul/Downloads/gcp-assignment-322710-6d632b671846.json')

        cli = bigquery.Client(project=project_id, credentials=credentials)

        table_id = "{}.{}.{}".format(project_id, dataset_id, table_name)

        table = bigquery.Table(table_id, schema=schema)
        table = cli.create_table(table)  # Make an API request.raise Exception("Table creation failed")
        print(
            "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
        )

    except Exception as err:

        print(err.args)
        raise Exception("Table creation failed")


def load_data(project_id, dataset_id, table_name):
    try:

        res = pd.read_csv("finalresult.csv")

        print(res)
        table_id = "{}.{}".format(dataset_id, table_name)
        # print(table_id)
        res.to_gbq(table_id, project_id, if_exists="replace")
        print("Data loaded successfully")




    except Exception as err:

        print(err.args)
        raise Exception("Loading Data failed")


def solution():
    try:

        download_file_from_bucket("Customers.csv", os.path.join(os.getcwd(), 'customersfile.csv'));
        download_file_from_bucket("Orders.csv", os.path.join(os.getcwd(), 'ordersfile.csv'));
        print("ALL Files Downloaded")

        join_files_to_final_csv();

        # create_dataset("gcp-assignment-322710");

        schema = [
            bigquery.SchemaField("CustomerID", "INTEGER"),
            bigquery.SchemaField("CustomerName", "STRING"),
            bigquery.SchemaField("ContactName", "STRING"),
            bigquery.SchemaField("Address", "STRING"),
            bigquery.SchemaField("City", "STRING"),
            bigquery.SchemaField("PostalCode", "STRING"),
            bigquery.SchemaField("Country", "STRING"),
            bigquery.SchemaField("OrderID", "FLOAT"),
            bigquery.SchemaField("EmployeeID", "FLOAT"),
            bigquery.SchemaField("OrderDate", "DATE"),
            bigquery.SchemaField("ShipperID", "FLOAT"),
        ]

        # create_table("gcp-assignment-322710", "first_dataset", "rahul", schema);

        load_data("gcp-assignment-322710", "first_dataset", "rahul")






    except Exception as err:
        print(err.args)


if __name__ == '__main__':
    solution()
