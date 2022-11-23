from time import time
from csv import DictReader
from elasticsearch import Elasticsearch, helpers

CERT_FINGERPRINT = "29:91:51:33:95:41:8E:EF:BB:F1:F4:CE:96:85:23:F8:B0:7B:E9:CF:10:7A:0C:9E:6D:1C:4D:9F:C4:3B:77:E6"
ELASTIC_PASSWORD = "Sm*U1IJ7Mbcngor9pRiI"

client = Elasticsearch(
    "https://localhost:9200",
    ssl_assert_fingerprint=CERT_FINGERPRINT,
    basic_auth=("elastic", ELASTIC_PASSWORD)
)

print('Connected to Elasticsearch')


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


def rating_generator():
    with open('BX-Book-Ratings.csv', encoding="utf-8") as file:
        for rating in DictReader(file):
            yield {
                "_index": 'ratings',
                "user_id": rating['uid']
            }


print('Indexing... (1/3)')
t0 = time()
res = helpers.bulk(client=client, actions=book_generator())
t1 = time()
print(f'Indexed {res[0]} documents in {t1 - t0}s')
