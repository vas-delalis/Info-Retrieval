from time import time
from csv import DictReader
from elasticsearch import helpers
from elasticsearch_client import client
import pandas as pd
import sqlite3

connection = sqlite3.connect("db.sqlite3")
cursor = connection.cursor()

print('Adding data to DB...')

# Users
cursor.execute('DROP TABLE IF EXISTS users')
cursor.execute('CREATE TABLE users(id INTEGER PRIMARY KEY, age INTEGER, country VARCHAR, cluster INTEGER)')

users = pd.read_csv('BX-Users.csv', index_col='uid')
users['country'] = users['location'].str.split(',').str.get(2).str.strip()
users = users.drop(columns=['location'])
users.index = users.index.rename('id')
users.to_sql('users', con=connection, if_exists='append')


# User ratings
cursor.execute('DROP TABLE IF EXISTS user_ratings')
cursor.execute('CREATE TABLE user_ratings '
                '(user_id INTEGER, isbn VARCHAR, rating INTEGER, '
                'is_cluster_avg INTEGER, PRIMARY KEY (user_id, isbn))')

user_ratings = pd.read_csv('BX-Book-Ratings.csv').rename(columns={'uid': 'user_id'})
user_ratings['is_cluster_avg'] = 0
user_ratings.to_sql('user_ratings', con=connection, if_exists='append', index=False)

cursor.execute('DROP TABLE IF EXISTS cluster_ratings')
cursor.execute('CREATE TABLE cluster_ratings '
                '(cluster INTEGER, isbn VARCHAR, avg_rating INTEGER, '
                'PRIMARY KEY (cluster, isbn))')

connection.close()

# Elasticsearch
def book_generator():
    with open('BX-Books.csv', encoding="utf-8") as file:
        for book in DictReader(file):
            yield {
                "_index": 'books',
                "_id": book['isbn'],
                "title": book["book_title"],
                "author": book["book_author"],
                "year_of_publication": book["year_of_publication"],
                "publisher": book["publisher"],
                "summary": book["summary"],
                "category": book['category'][2:-2]
            }


# print('Indexing books into Elasticsearch...')
# t0 = time()
# res = helpers.bulk(client=client, actions=book_generator())
# t1 = time()
# print(f'Indexed {res[0]} books in {t1 - t0}s')
