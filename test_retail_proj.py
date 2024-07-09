#import pytest
from lib.Utils import get_spark_session
from lib.DataReader import *
from lib.DataManipulation import *
from lib.ConfigReader import *

# write setup code here



def test_read_customer_df(spark):
    customers_count = read_customers(spark,"LOCAL").count()
    assert customers_count ==12435


def test_read_orders_df(spark):
    orders_count = read_orders(spark,"LOCAL").count()
    assert orders_count ==68884

# def test_filter_closed_orders():
#     spark = get_spark_session("LOCAL")
#     orders_df = read_customers(spark,"LOCAL")
#     filtered_closed_df_count = filter_closed_orders(orders_df)
#     assert orders_count ==7556


def test_read_app_config():
    config = get_app_config("LOCAL")
    assert config["orders.file.path"] == "data/orders.csv"

