import pandas as pd
import numpy as np
from time import *
from prefect import flow, get_run_logger, task, serve
from time import *
from prefect.deployments import Deployment

from crawling.trading_binance import get_api
from crawling.hose import hose
from connect_db.connect_duckdb import *
from typing import Annotated
from datetime import date, timedelta

today = date.today()
today = today - timedelta(days=2)
today = today.strftime("%d%m%Y")


class Binance():

    def __init__(self) -> None:
        pass

    def get_data(self) -> Annotated[list[tuple[any]], 'data binance']:
        data = get_api()
        return data

    def insert_data(self, data):
        create_insert_table_binance(data=data)


class Hose():

    def __init__(self, today):
        self.start_date = today
        self.end_date = today

    def get_data(self) -> Annotated[list[tuple[any]], 'Data Hose']:
        data = hose(start_date=self.start_date, end_date=self.end_date)
        return data

    def insert_data(self, data):
        create_insert_table_hose(data=data)


# @task(name = 'task_crawling_data_binance', retry_delay_seconds = 5, log_prints=True)
def flow_crawling_data_binance():
    binance = Binance()
    data = binance.get_data()
    print(data)
    binance.insert_data(data)


def flow_crawling_data_hose():
    hose = Hose(today=today)
    data = hose.get_data()
    print(data)
    hose.insert_data(data)


flow_crawling_data_hose()
