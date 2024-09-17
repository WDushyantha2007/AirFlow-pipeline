import pandas as pd
from airflow.providers.mysql.hooks.mysql import MySqlHook

def extract_data():

    retail_sale_data = pd.read_csv('../dataset/retail_sales_dataset.csv')
    retail_sale_data_filtered = retail_sale_data[["Transaction ID", "Date", "Customer ID", "Product Category", "Quantity", "Price per Unit", "Total Amount"]]
    retail_sale_data_filtered.dropna()
    transform_data(retail_sale_data_filtered);



def transform_data(retail_sale_data_filtered):
    print(retail_sale_data_filtered)
    retail_sale_data_filtered = retail_sale_data_filtered.to_csv(index=None, header=None)
    mysql_sql_upload = MySqlHook(mysql_conn_id="mysql_conn")
    mysql_sql_upload.bulk_load('retails', mysql_sql_upload)
    return True

extract_data()