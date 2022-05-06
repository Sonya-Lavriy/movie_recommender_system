import requests
import pandas as pd
import re
from string import punctuation
from bs4 import BeautifulSoup
from sqlalchemy import create_engine


URL = 'https://www.imdb.com/find?q=Toy+Story&ref_=nv_sr_sm'
HEADERS = {
    'user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36',
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9'}


def form_url(title):
    words_list = [word for word in re.split('\W+', title.split('(')[0]) if word != '']
    if words_list[-1] == 'The' or words_list[-1] == 'A':
        words_list.pop()
    # print(words_list)
    construction = "+".join(words_list) + '+' + re.findall('\d{4}', title)[0]
    url = f'https://www.imdb.com/find?q={construction}&ref_=nv_sr_sm'
    print(url)
    return url


def get_html(url):
    r = requests.get(url, headers=HEADERS)
    return r


def get_content(html):
    soup = BeautifulSoup(html, 'html.parser')
    tr = soup.findAll('tr', class_='findResult odd')[0]
    film_url = 'https://www.imdb.com' + tr.find('td', class_="primary_photo").find('a').get_attribute_list('href')[0]
    poster_url = tr.find('td', class_="primary_photo").find('a').find('img').get_attribute_list('src')[0]
    ua_title = tr.find('td', class_="result_text").find('a').get_text()
    return film_url, poster_url, ua_title


def parse(url):
    html = get_html(url)
    if html.status_code == 200:
        return get_content(html.text)
    else:
        print('Something wrong :(')


movies_df = pd.read_csv('data/raw/100k/movies.csv')
movies_df.drop([266], axis=0, inplace=True)
movies_df.drop(['Unnamed: 0'], axis=1, inplace=True)

# title = movies_df.loc[11, 'title']
# print(title)
# words_list = [word for word in re.split('\W+', title.split('(')[0]) if word != '']
# print(words_list)
# if words_list[-1] == 'The':
#     words_list.pop()
# construction = "+".join(words_list) + '+' + re.findall('\d{4}', title)[0]
# url = f'https://www.imdb.com/find?q={construction}&ref_=nv_sr_sm'
# print(url)


title_ua_list = []
movies_url_list = []
banner_url_list = []
for row in movies_df.index:
    try:
        print(movies_df.loc[row, 'title'])
        URL = form_url(movies_df.loc[row, 'title'])
        movie_url, banner_url, title_ua = parse(URL)
        print(title_ua)
        print(movie_url)
        print(banner_url)
        title_ua_list.append(title_ua)
        movies_url_list.append(movie_url)
        banner_url_list.append(banner_url)
        print('finish {} iteration'.format(row))
        print('-------------------')
    except:
        title_ua_list.append(movies_df.loc[row, 'title'])
        movies_url_list.append(None)
        banner_url_list.append(None)


movies_df['title_ua'] = title_ua_list
movies_df['imdb_url_new'] = movies_url_list
movies_df['poster_url'] = banner_url_list

engine = create_engine('postgresql://postgres:087539@localhost:5432/RecSys')
movies_df.to_sql('full_movies', engine, if_exists='append', index=False)


# print(title_ua_list[:10])
