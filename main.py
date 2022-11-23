import csv
from collections import defaultdict
from elasticsearch import Elasticsearch

CERT_FINGERPRINT = "29:91:51:33:95:41:8E:EF:BB:F1:F4:CE:96:85:23:F8:B0:7B:E9:CF:10:7A:0C:9E:6D:1C:4D:9F:C4:3B:77:E6"
ELASTIC_PASSWORD = "Sm*U1IJ7Mbcngor9pRiI"

client = Elasticsearch(
    "https://localhost:9200",
    ssl_assert_fingerprint=CERT_FINGERPRINT,
    basic_auth=("elastic", ELASTIC_PASSWORD)
)


def get_books():
    user_id = input('User id: ')
    query = input('Search query: ')

    # Read ratings
    user_ratings = defaultdict(int)
    with open('BX-Book-Ratings.csv', encoding="utf-8") as file:
        for row in csv.reader(file):
            if row[0] == user_id:
                user_ratings[row[1]] = int(row[2])

    # Search
    result = client.search(index="books", query={'multi_match': {'query': query, 'fields': ['title^2', 'summary']}},
                           source=False, size=10000)
    max_score = result['hits']['max_score']
    hits = result['hits']['hits']

    # Process
    personalized_hits = []
    for hit in hits:
        # Map rating from [0, 10] to [0, max_score * 25%] and add it to the ES score
        rating_factor = user_ratings[hit['_id']] * 0.25 * (max_score / 10)
        score = hit['_score'] + rating_factor
        personalized_hits.append({
            'id': hit['_id'],
            'personalized_score': score,
            'rating_factor': rating_factor
        })

    personalized_hits.sort(key=lambda x: x['personalized_score'], reverse=True)

    return personalized_hits[:len(personalized_hits) // 10]


books = get_books()
print()
print(f'Showing {len(books)} books:')
print()
print('ISBN        Personalized score  From rating')
print('----------  ------------------  -----------')
for book in books:
    from_rating = f'{book["rating_factor"]:12.2f}' if book['rating_factor'] else '          --'
    print(f'{book["id"]} {book["personalized_score"]:19.2f} {from_rating}')
