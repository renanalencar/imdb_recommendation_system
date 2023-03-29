# -*- coding: utf-8 -*-
# Reference: https://jovian.com/mihirpanchal0072/web-scraping-imdb-database-for-movies-using-python-beautiful-soup
import os
import re
import json
import string
import requests
import pandas as pd

from bs4 import BeautifulSoup
from pymongo import MongoClient, errors, ASCENDING
from dotenv import find_dotenv, load_dotenv

'''List of popular genres'''
GENRE_LIST = ['action', 'adventure', 'animation', 'biography', 'comedy', 'crime', 'documentary', 'drama', 'family', 'fantasy', 'film-noir', 'history', 'horror', 'music', 'musical', 'mystery', 'romance', 'sci-fi', 'sport', 'superhero', 'thriller', 'war', 'western']

'''List of popular TV parental ratings'''
TV_PARENTAL_RATING = ['TV-Y', 'TV-Y7', 'TV-G', 'TV-PG', 'TV-14', 'TV-MA']

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
def get_uid(doc):
    uid_selector="loadlate"            
    movie_uid_tags=doc.find_all('img',{'class':uid_selector})
    movie_uid=[]
    for tag in movie_uid_tags:
        uid = tag.attrs['data-tconst']
        movie_uid.append(uid)
    return movie_uid

'''Function to get the movie rank from the page content'''
def get_rank(doc):
    rank_selector="lister-item-index unbold text-primary"            
    movie_rank_tags=doc.find_all('span',{'class':rank_selector})
    movie_rank=[]
    for tag in movie_rank_tags:
        rank = tag.get_text().strip('.')
        movie_rank.append(rank)
    return movie_rank

'''Function to get the movie name from the page content'''
def get_movie_name(doc):
    selection_class="lister-item-header"
    movie_name_tags=doc.find_all('h3',{'class':selection_class})
    movie_name=[]

    for tag in movie_name_tags:
        name = tag.find('a').text
        movie_name.append(name) 
        
    return movie_name

'''Function to get the movie year from the page content'''
def get_movie_year(doc):
    year_selector = "lister-item-year text-muted unbold"           
    movie_year_tags=doc.find_all('span',{'class':year_selector})
    movie_year=[]
    for tag in movie_year_tags:
        movie_year.append(tag.get_text().strip("()"))
    return movie_year

'''Function to get the movie certificate from the page content'''
def get_certificate(doc):
    
    selection_class="certificate"
    movie_certificate_tags=doc.find_all('span',{'class':selection_class})
    movie_certificate=[]

    for tag in movie_certificate_tags:
        certificate = tag.text[:10]
        movie_certificate.append(certificate)
    
    return movie_certificate

'''Function to get the movie duration from the page content'''
def get_duration(doc):
    
    selection_class = "runtime"
    movie_duration_tags=doc.find_all('span',{'class':selection_class})
    movie_duration=[]
    
    for tag in movie_duration_tags:
        duration = tag.text[:10].replace(' min','')
        movie_duration.append(int(duration))
    
    return movie_duration

'''Function to get the movie genre from the page content'''
def get_genre(doc):
    
    selection_class = "genre"
    movie_genre_tags=doc.find_all('span',{'class':selection_class})
    movie_genre=[]
    
    for tag in movie_genre_tags:
        genre = re.sub('['+string.punctuation+']', '', tag.get_text().lower()).split()
        movie_genre.append(genre)
    
    return movie_genre

'''Function to get the movie rating from the page content'''
def get_rating(doc):
    
    rating_selector="inline-block ratings-imdb-rating"            
    movie_rating_tags=doc.find_all('div',{'class':rating_selector})
    movie_rating=[]
    for tag in movie_rating_tags:
        rating = float(tag.get_text().strip())
        movie_rating.append(rating)

    return movie_rating

'''Function to get the movie director from the page content'''
def get_director(doc):
    
    selection_class = ""
    movie_director_tags=doc.find_all('p',{'class':selection_class})
    movie_director=[]
    
    for tag in movie_director_tags:
        director = tag.find('a').text
        if not "|" in tag.text:
            director = ""    
        movie_director.append(director)
    
    return movie_director

'''Function to get the movie stars from the page content'''
def get_stars(doc):
    
    selection_class = ""
    movie_stars_tags=doc.find_all('p',{'class':selection_class})
    movie_stars=[]
    
    for tag in movie_stars_tags:
        stars = []
        links = tag.find_all('a', limit=5)
        
        ghost_tag = tag.find('span',{'class':'ghost'})
        if ghost_tag is not None:
            links.pop(0)
        
        stars = [format(link.text) for link in links]
        movie_stars.append(stars)
        
    return movie_stars

'''Function to get the movie number of votes from the page content'''
def get_num_votes(doc):
    
    votes_selector="sort-num_votes-visible"            
    movie_votes_tags=doc.find_all('p',{'class':votes_selector})
    movie_votes=[]

    for tag in movie_votes_tags:
        num_votes = 0
        if tag.find('span',{'name':'nv'}) is not None:
            num_votes = tag.find('span',{'name':'nv'}).attrs['data-value']
        movie_votes.append(int(num_votes))

    return movie_votes

'''Function to get the movie data from the page content'''
def imdb_dict():
    # Let's we create a dictionary to store data of all movies
    movies_dictionary={
        'uid':[],
        'rank':[],
        'name':[],
        'year':[],
        'certificate':[],
        'duration':[],
        'genre':[],
        'rating':[],
        'director':[],
        'stars':[],
        'num_votes':[],
    }

    # We have to scrap more than one page so we want urls of all pages with the help of loop we can get all urls
    for i in range(1,2000,50):
        # Parse using BeautifulSoup
        try:
            doc = get_topics_page('crime')
        except Exception as e:
            print(e)
            break

        uid = get_uid(doc)
        # print(len(uid))
        rank = get_rank(doc)
        # print(len(rank))
        name = get_movie_name(doc)
        # print(len(name))
        year = get_movie_year(doc)
        # print(len(year))
        certificate = get_certificate(doc)
        # print(certificate)
        # print(len(certificate))
        duration = get_duration(doc)
        # print(duration)
        # print(len(duration))
        genre = get_genre(doc)
        # print(len(genre))
        rating = get_rating(doc)
        # print(len(rating))
        director = get_director(doc)
        # print(len(director))
        stars = get_stars(doc)
        # print(len(stars))
        num_votes = get_num_votes(doc)
        # print(len(num_votes))

        # We are adding every movie data to dictionary
        for i in range(len(name)):
            movies_dictionary['uid'].append(uid[i])
            movies_dictionary['rank'].append(rank[i])
            movies_dictionary['name'].append(name[i])
            movies_dictionary['year'].append(year[i])
            movies_dictionary['certificate'].append(certificate[i])
            movies_dictionary['duration'].append(duration[i])
            movies_dictionary['genre'].append(genre[i])
            movies_dictionary['rating'].append(rating[i])
            movies_dictionary['director'].append(director[i])
            movies_dictionary['stars'].append(stars[i])
            movies_dictionary['num_votes'].append(num_votes[i])
        
    return pd.DataFrame(movies_dictionary)

'''Function to remove duplicate movies from the dataframe'''
def remove_duplicates(df):
    # We are removing duplicate movies
    df.drop_duplicates(subset='uid', keep='first', inplace=True)
    return df

'''Function to clean the data'''
def clean_data(df):
    # We are cleaning data
    df = remove_duplicates(df)
    df['year'] = df['year'].astype(int)
    df['duration'] = df['duration'].astype(int)
    df['rating'] = df['rating'].astype(float)
    df['num_votes'] = df['num_votes'].astype(int)
    return df

'''Function to save only movies in the dataframe'''
def save_only_movies(df):
    # We are saving only movies
    df = df[~df['certificate'].isin(TV_PARENTAL_RATING)]
    return df

'''Function to save data to database'''
def save_to_db(df):
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

    try:
        movie_id = movies.insert_many(json_obj)
    except errors.BulkWriteError as bwe:
        print(bwe.details)
    finally:
        print(movie_id)

'''Function to carry out a single page test'''
def single_page_test():
    doc = get_topics_page('crime')
    uid = get_uid(doc)
    rank = get_rank(doc)
    name = get_movie_name(doc)
    year = get_movie_year(doc)
    certificate = get_certificate(doc)
    duration = get_duration(doc)
    genre = get_genre(doc)
    rating = get_rating(doc)
    director = get_director(doc)
    stars = get_stars(doc)
    votes = get_num_votes(doc)

    movie_dictionary={
        'uid':uid,
        'rank':rank,
        'name':name,
        'year':year,
        'certificate':certificate,
        'duration':duration,
        'genre':genre,
        'rating':rating,
        'director':director,
        'stars':stars,
        'num_votes':votes,
    }

    df = pd.DataFrame.from_dict(movie_dictionary,orient='index')
    df = df.transpose()
    print(df)

    save_to_db(df)

'''Function to carry out the main program'''
def main():
    df = imdb_dict()
    df = clean_data(df)
    df = save_only_movies(df)
    save_to_db(df)

if __name__ == "__main__":

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    load_dotenv(find_dotenv())

    single_page_test()
    # main()
