import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed


class IMDBCrawler:
    def __init__(self):
        self.baseURL = 'https://www.imdb.com/'

    def start_crawling(self):
        pass


if __name__ == '__main__':
    crawler = IMDBCrawler()
    crawler.start_crawling()
