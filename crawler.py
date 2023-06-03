import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed
import os


class IMDBCrawler:
    def __init__(self):
        self.baseURL = 'https://www.imdb.com/'
        self.data_path = './Data'

    def start_crawling(self):
        self.create_data_directory()

    def create_data_directory(self):
        if not os.path.exists(self.data_path):
            os.makedirs(self.data_path)

    def load_csv(self, df_name):
        file_path = os.path.join(self.data_path, df_name + '.csv')

        if not os.path.exists(file_path):
            df_columns = getattr(self, df_name).columns
            df = pd.DataFrame(columns=df_columns)
            df.to_csv(file_path, index=False)

        setattr(self, df_name, pd.read_csv(file_path))

    def save_csv(self, df_name):
        file_path = os.path.join(self.data_path, df_name + '.csv')
        df = getattr(self, df_name)
        df.to_csv(file_path, index=False)


if __name__ == '__main__':
    crawler = IMDBCrawler()
    crawler.start_crawling()
