# -*- coding: utf-8 -*-
# Reference: https://jovian.com/mihirpanchal0072/web-scraping-imdb-database-for-movies-using-python-beautiful-soup
import os
import logging
import re
import json
import string
import requests
import pandas as pd

from bs4 import BeautifulSoup
from pymongo import MongoClient, errors, ASCENDING
from dotenv import find_dotenv, load_dotenv

'''List of popular genres'''
# GENRE_LIST = ['action', 'adventure', 'animation', 'biography', 'comedy', 'crime', 'documentary', 'drama', 'family', 'fantasy', 'film-noir', 'history', 'horror', 'music', 'musical', 'mystery', 'romance', 'sci-fi', 'sport', 'superhero', 'thriller', 'war', 'western']
GENRE_LIST = ['music', 'musical', 'mystery', 'romance', 'sci-fi', 'sport', 'superhero', 'thriller', 'war', 'western']

'''List of popular TV parental ratings'''
TV_PARENTAL_RATING = ['TV-Y', 'TV-Y7', 'TV-G', 'TV-PG', 'TV-14', 'TV-MA']

'''List of missing values'''
MISSING_VALUES = ['not certified', 'pre-production', 'post-production', 'filming', 'announced', 'completed']

'''Maximum number of pages per title genre'''
MAX_PAGES = 199

'''Logging object to log the messages'''
LOGGER = logging.getLogger(__name__)

'''Function to get the page content of the topic'''
def get_topics_page(genre, page_number=None):
    
    topic_url = 'https://www.imdb.com/search/title/?genres='+genre+'&start='+str(page_number)+'&explore=title_type,genres&ref_=adv_nxt'
    
    if page_number == None:
        topic_url = 'https://www.imdb.com/search/title/?genres='+genre+'&explore=title_type,genres&ref_=adv_prv'

    elif page_number == 1:
        topic_url = 'https://www.imdb.com/search/title/?genres='+genre+'&start=51&explore=title_type,genres&ref_=adv_nxt'
    
    headers = {'Accept-Language': 'en-US'}
    response=requests.get(topic_url, headers=headers)

    # to check whether the response is successful or not
    if response.status_code != 200:                    
        raise Exception(f'Failed to load page {topic_url}')
    
    # Parse using BeautifulSoup
    doc = BeautifulSoup(response.text, 'html.parser')
    
    return doc

'''Function to get the movie uid from the page content'''
def get_movie_uid(doc):
    uid_selector = "loadlate"            
    movie_uid_tags = doc.find_all('img',{'class':uid_selector})
    movie_uid = []

    for tag in movie_uid_tags:
        uid = tag.attrs['data-tconst']
        movie_uid.append(uid)

    return movie_uid

'''Function to get the movie rank from the page content'''
def get_movie_rank(doc):
    rank_selector = "lister-item-index unbold text-primary"            
    movie_rank_tags = doc.find_all('span',{'class':rank_selector})
    movie_rank = []

    for tag in movie_rank_tags:
        rank = tag.get_text().strip('.')
        rank = rank.replace(',', '')
        movie_rank.append(rank)

    return movie_rank

'''Function to get the movie name from the page content'''
def get_movie_name(doc):
    selection_class = "lister-item-header"
    movie_name_tags = doc.find_all('h3',{'class':selection_class})
    movie_name = []

    for tag in movie_name_tags:
        name = tag.find('a').text
        movie_name.append(name) 
        
    return movie_name

'''Function to get the movie year from the page content'''
def get_movie_year(doc):
    year_selector = "lister-item-year text-muted unbold"           
    movie_year_tags = doc.find_all('span',{'class':year_selector})
    movie_year = []

    for tag in movie_year_tags:
        # get only the year from the text
        year = re.search(r'\d+', tag.get_text())
        if year is not None:
            year = year.group()
            year = year.replace(',', '')

        if year is None:
            year = '0'

        movie_year.append(year)

    return movie_year

'''Function to get the movie certificate from the page content'''
def get_movie_certificate(doc):
    # get the first sibling p.text-muted of the h3.lister-item-header 
    movie_feature_tags = doc.select('h3.lister-item-header + p.text-muted')
    movie_certificate = []

    for tag in movie_feature_tags:
        certificate = 'not certified'
        # get the span.certificate right inside the p.text-muted
        content = tag.select_one('p.text-muted > span.certificate')
        
        # check if it is in post-production
        production = tag.select_one('p.text-muted > span.ghost + b')
        if production is not None:
            certificate = production.get_text().lower()
        # save the certificate if it is not missing
        if content is not None:
            certificate = content.text[:10]

        movie_certificate.append(certificate)
    
    return movie_certificate


'''Function to get the movie runtime from the page content'''
def get_movie_runtime(doc):
    # get the first sibling p.text-muted of the h3.lister-item-header 
    movie_feature_tags = doc.select('h3.lister-item-header + p.text-muted')
    movie_runtime = []

    for tag in movie_feature_tags:
        runtime = '0'
        # get the span.runtime right inside the p.text-muted
        content = tag.select_one('p.text-muted > span.runtime')

        if content is not None:
            runtime = content.text[:10].replace(' min','')
            runtime = runtime.replace(',', '')
            
        movie_runtime.append(runtime)
    
    return movie_runtime

'''Function to get the movie genre from the page content'''
def get_movie_genre(doc):
    selection_class = "genre"
    movie_genre_tags = doc.find_all('span',{'class':selection_class})
    movie_genre = []
    
    for tag in movie_genre_tags:
        genre = re.sub('['+string.punctuation+']', '', tag.get_text().lower()).split()
        movie_genre.append(genre)
    
    return movie_genre

'''Function to get the movie rating from the page content'''
def get_movie_rating(doc):
    movie_feature_tags = doc.select('div.lister-item-content')
    movie_rating = []

    for tag in movie_feature_tags:
        rating = '0.0'

        # get the div.ratings-bar right after the p.text-muted
        content = tag.select_one('p.text-muted + div.ratings-bar')
        if content is not None:
            rating = content.get_text().strip().split('\n')[0]
            rating = re.search(r'\d+\.\d+', rating)

            if rating is not None:
                rating = rating.group()
                rating = rating.replace(',', '')
            if rating is None:
                rating = '0.0'
                
        movie_rating.append(rating)
    
    return movie_rating

'''Function to get the movie director from the page content'''
def get_movie_director(doc):
    selection_class = ""
    movie_director_tags = doc.find_all('p',{'class':selection_class})
    movie_director = []
    
    for tag in movie_director_tags:
        director = ""
        if "|" in tag.text:
            director = tag.find('a').text

        movie_director.append(director)
    
    return movie_director

'''Function to get the movie stars from the page content'''
def get_movie_stars(doc):
    selection_class = ""
    movie_stars_tags = doc.find_all('p',{'class':selection_class})
    movie_stars = []
    
    for tag in movie_stars_tags:
        stars = []
        # get all links limited to top 5
        links = tag.find_all('a', limit=5)
        
        ghost_tag = tag.find('span',{'class':'ghost'})
        if ghost_tag is not None:
            links.pop(0)
        
        stars = [format(link.text) for link in links]
        movie_stars.append(stars)
        
    return movie_stars

'''Function to get the movie number of votes from the page content'''
def get_movie_num_votes(doc):
    movie_feature_tags = doc.select('div.lister-item-content')
    movie_votes = []

    for tag in movie_feature_tags:
        num_votes = '0'
        # get the span[name="nv"] right after the div.ratings-bar
        content = tag.select_one('p.sort-num_votes-visible > span[name="nv"]')
        if content is not None:
            num_votes_tag = content.attrs['data-value']
            num_votes = num_votes_tag.replace(',', '')

        movie_votes.append(num_votes)
    
    return movie_votes

'''Function to get the movie data from the page content'''
def imdb_dict(genre_search, num_pages=1):
    LOGGER.info('scraping IMDB movies for \'%s\'...' % genre_search)

    # Let's we create a dictionary to store data of all movies
    movies_dictionary={
        'uid':[],
        'rank':[],
        'name':[],
        'year':[],
        'certificate':[],
        'runtime':[],
        'genre':[],
        'rating':[],
        'director':[],
        'stars':[],
        'num_votes':[],
    }

    # We have to scrap more than one page so we want urls of all pages with the help of loop we can get all urls
    # each page has 50 movies so we have to get 50 movies from each page (1~51)
    num_titles = (num_pages * 50) + 2
    for i in range(51, num_titles, 50):
        LOGGER.info('[genre: \'%s\', page: %d/%d]' % (genre_search, i/50, num_titles/50))
        # Parse using BeautifulSoup
        try:
            doc = get_topics_page(genre_search, i)
        except Exception as e:
            LOGGER.error(e)
            break

        uid = get_movie_uid(doc)
        rank = get_movie_rank(doc)
        name = get_movie_name(doc)
        year = get_movie_year(doc)
        certificate = get_movie_certificate(doc)
        runtime = get_movie_runtime(doc)
        genre = get_movie_genre(doc)
        rating = get_movie_rating(doc)
        director = get_movie_director(doc)
        stars = get_movie_stars(doc)
        num_votes = get_movie_num_votes(doc)

        # We are adding every movie data to dictionary
        for i in range(len(name)):
            movies_dictionary['uid'].append(uid[i])
            movies_dictionary['rank'].append(rank[i])
            movies_dictionary['name'].append(name[i])
            movies_dictionary['year'].append(year[i])
            movies_dictionary['certificate'].append(certificate[i])
            movies_dictionary['runtime'].append(runtime[i])
            movies_dictionary['genre'].append(genre[i])
            movies_dictionary['rating'].append(rating[i])
            movies_dictionary['director'].append(director[i])
            movies_dictionary['stars'].append(stars[i])
            movies_dictionary['num_votes'].append(num_votes[i])
        
    return pd.DataFrame(movies_dictionary)

'''Function to remove duplicate movies from the data-frame'''
def remove_duplicates(df):
    LOGGER.info('removing duplicate data...')

    # We are removing duplicate movies
    df.drop_duplicates(subset='uid', keep='first', inplace=True)

    return df

'''Function to clean the data'''
def clean_data(df):
    LOGGER.info('cleaning data...')

    # We are cleaning data
    df = remove_duplicates(df)

    df['rank'] = df['rank'].astype(int)
    df['year'] = df['year'].astype(int)
    df['runtime'] = df['runtime'].astype(int)
    df['rating'] = df['rating'].astype(float)
    df['num_votes'] = df['num_votes'].astype(int)

    return df

'''Function to save only movies in the data-frame'''
def save_only_movies(df):
    LOGGER.info('letting only movies onto dataframe...')

    # We are saving only movies
    df = df[~df['certificate'].isin(TV_PARENTAL_RATING)]
    df = df[~df['certificate'].isin(MISSING_VALUES)]
    return df

'''Function to save data to json file'''
def save_to_json(df):
    LOGGER.info('saving data to json file...')

    # We are saving data to json file
    df.to_json('./data/raw/movies.json', orient='records', lines=True)

'''Function to save data to database'''
def save_to_db(df):
    LOGGER.info('saving data to database...')

    # We are saving data to database
    json_obj = json.loads(df.to_json(orient='records'))
    # Making a Connection with MongoClient
    client = MongoClient(os.getenv('MONGODB_URI'))
    # database
    db = client["imdb"]
    # collection
    movies = db["movies"]
    # creating index
    movies.create_index([('uid', ASCENDING)], unique=True)
    
    if json_obj == []:
        LOGGER.info('no movies to insert')
        return
    
    num_insertions = 0
    try:
        result = movies.insert_many(json_obj, ordered=False).inserted_ids
        num_insertions = len(result)
    except errors.BulkWriteError as bwe:
        write_errors = bwe.details['writeErrors']
        for we in write_errors:
            LOGGER.error(we['errmsg'])
        num_insertions = bwe.details['nInserted']
    finally:
        client.close()
        LOGGER.info('%d movies inserted' % num_insertions)

'''Function to carry out a single page test'''
def single_page_test():
    LOGGER.info('starting single test page.')

    for genre in ['crime']:
        df = imdb_dict(genre)

        df = save_only_movies(df)
        df = clean_data(df)

        save_to_json(df)
        save_to_db(df)

'''Function to carry out the main program'''
def main():
    """ Runs data processing scripts to turn raw data from (../raw) into
        cleaned data ready to be analyzed (saved in ../processed).
    """
    LOGGER.info('getting data from IMDB website.')
    
    for genre in GENRE_LIST:
        df = imdb_dict(genre, MAX_PAGES)
        df = save_only_movies(df)
        df = clean_data(df)

        save_to_json(df)
        save_to_db(df)

if __name__ == "__main__":
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    load_dotenv(find_dotenv())

    main()
    # single_page_test()
