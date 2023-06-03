import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed
import os
import re
import datetime


class IMDBCrawler:
    def __init__(self):
        self.baseURL = 'https://www.imdb.com'
        self.data_path = './Data'
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'
            , 'accept-language': 'en-US'
        }
        self.dataframe_names = ['movies_url', 'movie', 'person', 'genre', 'cast', 'crew', 'crawled_movies_url']
        self.movies_url = pd.DataFrame(columns=['id', 'url'])
        self.movie = pd.DataFrame(
            columns=['id', 'title', 'year', 'runtime', 'parental_guide', 'budget', 'gross_us_canada',
                     'gross_worldwide'])
        self.person = pd.DataFrame(columns=['id', 'name'])
        self.genre = pd.DataFrame(columns=['movie_id', 'genre'])
        self.cast = pd.DataFrame(columns=['movie_id', 'person_id'])
        self.crew = pd.DataFrame(columns=['movie_id', 'person_id', 'role'])
        self.crawled_movies_url = pd.DataFrame(columns=['id', 'url'])

    def start_crawling(self):
        self.create_data_directory()

        for df_name in self.dataframe_names:
            self.load_csv(df_name)

        self.get_top250_movies_urls()
        self.crawl_movies_url()

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
                    movie_id = url.split('/')[2]
                    self.movies_url.loc[len(self.movies_url)] = [movie_id, url]

                self.save_csv('movies_url')
                break

            except Exception as e:
                req_failed_count += 1
                print(e)

        print("get_top250_movies_urls finished")

    def scrap_movie_page(self, url):
        movie_details = {'id': url.split('/')[2]}
        cast = []
        crew = []
        person = []
        genre = []
        new_url = self.baseURL + url
        req_failed_count = 0
        while req_failed_count < 3:
            try:
                response = requests.get(new_url, headers=self.headers, timeout=10)
                if response.status_code != 200:
                    raise Exception(f"status code is: {response.status_code}")

                soup = BeautifulSoup(response.content, 'html.parser')
                movie_details['title'] = soup.find(class_="sc-afe43def-1").text

                movie_details['year'] = np.nan
                movie_details['runtime'] = np.nan
                movie_details['parental_guide'] = np.nan
                for item in soup.find(class_="sc-afe43def-4").find_all('li'):
                    if item.text.isdigit():
                        movie_details['year'] = int(item.text)
                    elif re.match("(\d*m|\d*h|\d*h \d*m)", item.text):
                        if ' ' in item.text:
                            runtime = datetime.datetime.strptime(item.text, '%Hh %Mm')
                        elif 'h' in item.text:
                            runtime = datetime.datetime.strptime(item.text, '%Hh')
                        else:
                            runtime = datetime.datetime.strptime(item.text, '%Mm')
                        movie_details['runtime'] = runtime.hour * 60 + runtime.minute
                    else:
                        movie_details['parental_guide'] = item.text

                movie_details['budget'] = np.nan
                movie_details['gross_us_canada'] = np.nan
                movie_details['gross_worldwide'] = np.nan
                for item in soup.select('.sc-6d4f3f8c-2'):
                    if 'Budget' in item.text:
                        budget = ""
                        for c in item.find('div').text:
                            if c.isdigit():
                                budget += c
                        movie_details['budget'] = int(budget)
                    if 'Gross US & Canada' in item.text:
                        gross_us_canada = item.find('div').text
                        movie_details['gross_us_canada'] = int(gross_us_canada[1:].replace(',', ''))
                    elif 'Gross worldwide' in item.text:
                        gross_worldwide = item.find('div').text
                        movie_details['gross_worldwide'] = int(gross_worldwide[1:].replace(',', ''))

                for item in soup.select('.ipc-chip--on-baseAlt'):
                    genre.append({'movie_id': movie_details['id'], 'genre': item.text})

                for item in soup.find(class_='sc-52d569c6-3').find_all(class_='ipc-metadata-list__item'):
                    if 'Director' in item.text:
                        for director in item.find_all('li'):
                            person_id = director.find('a').get('href').split('/')[2]
                            crew.append({'movie_id': movie_details['id'], 'person_id': person_id, 'role': 'Director'})
                            person.append({'id': person_id, 'name': director.text})

                    elif 'Writers' in item.text:
                        for writer in item.find_all('li'):
                            person_id = writer.find('a').get('href').split('/')[2]
                            crew.append({'movie_id': movie_details['id'], 'person_id': person_id, 'role': 'Writer'})
                            person.append({'id': person_id, 'name': writer.text})

                for item in soup.select('.sc-bfec09a1-1'):
                    person_id = item.get('href').split('/')[2]
                    cast.append({'movie_id': movie_details['id'], 'person_id': person_id})
                    person.append({'id': person_id, 'name': item.text})

                return movie_details, genre, crew, cast, person, url
            except Exception as e:
                req_failed_count += 1
                print(new_url, e)
        return None, None, None, None, None, None

    def crawl_movies_url(self):
        print('crawl_movies_url started')
        self.movies_url.drop_duplicates()

        new_url = {}
        for i in range(len(self.movies_url)):
            new_url[self.movies_url.iloc[i]['id']] = self.movies_url.iloc[i]['url']
        print("count of loaded url: ", len(new_url))

        for i in range(len(self.crawled_movies_url)):
            if self.crawled_movies_url.iloc[i]['id'] in new_url:
                new_url.pop(self.crawled_movies_url.iloc[i]['id'])
        print("count of new url: ", len(new_url))

        with ThreadPoolExecutor(max_workers=50) as executor:
            futures = (executor.submit(self.scrap_movie_page, url) for url in new_url.values())
            for future in as_completed(futures):
                movie_details, genre, crew, cast, person, url = future.result()

                if movie_details is not None:
                    self.movie.loc[len(self.movie)] = [
                        movie_details['id'],
                        movie_details['title'],
                        movie_details['year'],
                        movie_details['runtime'],
                        movie_details['parental_guide'],
                        movie_details['budget'],
                        movie_details['gross_us_canada'],
                        movie_details['gross_worldwide']
                    ]
                    self.save_csv('movie')

                    for g in genre:
                        self.genre.loc[len(self.genre)] = [
                            g['movie_id'],
                            g['genre']
                        ]
                    self.save_csv('genre')

                    for c in crew:
                        self.crew.loc[len(self.crew)] = [
                            c['movie_id'],
                            c['person_id'],
                            c['role']
                        ]
                    self.save_csv('crew')

                    for c in cast:
                        self.cast.loc[len(self.cast)] = [
                            c['movie_id'],
                            c['person_id']
                        ]
                    self.save_csv('cast')

                    for p in person:
                        self.person.loc[len(self.person)] = [
                            p['id'],
                            p['name']
                        ]
                    self.save_csv('person')

                    self.crawled_movies_url.loc[len(self.crawled_movies_url)] = [url.split('/')[2], url]
                    self.save_csv('crawled_movies_url')


if __name__ == '__main__':
    crawler = IMDBCrawler()
    crawler.start_crawling()
