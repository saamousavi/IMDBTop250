import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed
import os

from pandas import DataFrame


class IMDBCrawler:
    def __init__(self):
        self.baseURL = 'https://www.imdb.com'
        self.data_path = './Data'
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'
            , 'accept-language': 'en-US'
        }
        self.dataframe_names = ['movies_url', 'movie', 'person', 'genre', 'cast', 'crew', 'crawled_movies_url']
        self.movies_url = pd.DataFrame(columns=['url'])
        self.movie = pd.DataFrame(
            columns=['id', 'title', 'year', 'runtime', 'parental_guide', 'budget', 'gross_us_canada',
                     'gross_worldwide'])
        self.person = pd.DataFrame(columns=['id', 'name'])
        self.genre = pd.DataFrame(columns=['movie_id', 'genre'])
        self.cast = pd.DataFrame(columns=['movie_id', 'person_id'])
        self.crew = pd.DataFrame(columns=['movie_id', 'person_id', 'role'])
        self.crawled_movies_url = pd.DataFrame(columns=['url'])

    def start_crawling(self):
        self.create_data_directory()

        for df_name in self.dataframe_names:
            self.load_csv(df_name)

        self.get_top250_movies_urls()

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

    def get_top250_movies_urls(self):
        print("get_top250_movies_urls started")

        url = self.baseURL + '/chart/top/?ref_=nv_mv_250'
        req_failed_count = 0
        while req_failed_count < 3:
            try:
                response = requests.get(url, headers=self.headers, timeout=10)
                if response.status_code != 200:
                    raise Exception(f"status code is: {response.status_code}")

                soup = BeautifulSoup(response.content, 'html.parser')
                for movie in soup.select('.titleColumn'):
                    url = movie.find('a').get('href')
                    self.movies_url.loc[len(self.movies_url)] = [url]

                self.save_csv('movies_url')

            except Exception as e:
                req_failed_count += 1
                print(e)

        print("get_top250_movies_urls finished")

    def scrap_movie_page(self, url):
        pass

    def crawl_movies_url(self):
        print('crawl_movies_url started')

        new_url = set()
        self.movies_url.apply(lambda row: new_url.add(row['url']), axis=1)
        print("count of loaded url: ", len(new_url))

        crawled_url = set()
        self.crawled_movies_url.apply(lambda row: crawled_url.add(row['url']), axis=1)

        new_url = new_url - crawled_url
        print(print("count of new url: ", len(new_url)))

        with ThreadPoolExecutor(max_workers=50) as executor:
            futures = (executor.submit(self.scrap_movie_page, url) for url in new_url)
            for future in as_completed(futures):
                pass


if __name__ == '__main__':
    crawler = IMDBCrawler()
    crawler.start_crawling()
