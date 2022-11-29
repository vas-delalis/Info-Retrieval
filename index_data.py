from time import time
from csv import DictReader
from elasticsearch import helpers
from es_connect import client


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


print('Indexing...')
t0 = time()
res = helpers.bulk(client=client, actions=book_generator())
t1 = time()
print(f'Indexed {res[0]} documents in {t1 - t0}s')
