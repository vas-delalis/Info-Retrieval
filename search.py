from elasticsearch_client import client
from time import time
import sqlite3

class Search:
    def __init__(self, connection: sqlite3.Connection, cursor: sqlite3.Cursor) -> None:
        self.connection = connection
        self.cursor = cursor

    def get_cluster_ratings(self, user_id: int):
        return dict(self.cursor.execute(
            'SELECT isbn, avg_rating '
            'FROM users JOIN cluster_ratings ON users.cluster = cluster_ratings.cluster '
            'WHERE id = ?',
            [user_id]
        ).fetchall())

    def get_user_ratings(self, user_id: int):
        return dict(self.cursor.execute(
            'SELECT isbn, rating FROM user_ratings WHERE user_id = ?',
            [user_id]
        ).fetchall())

    def get_books(self, user_id: int, query: str, include_cluster_ratings: bool):
        # Search
        result = client.search(
            index="books",
            query={'multi_match': {
                'query': query,
                'fields': ['title^2', 'summary']  # Title matches have double weight
            }},
            source=False,
            size=10000
        )
        max_score = result['hits']['max_score']
        hits = result['hits']['hits']

        t0 = time()
        ratings = self.get_user_ratings(user_id)
        if include_cluster_ratings:
            cluster_ratings = self.get_cluster_ratings(user_id)
            ratings = cluster_ratings | ratings  # Merge dicts (r.h.s. has priority)
        t1 = time()
        print('get ratings', t1-t0)

        t0 = time()
        # Process
        personalized_hits = []
        for hit in hits:
            if hit['_id'] in ratings:
                rating = ratings[hit['_id']]
            else:
                rating = 0

            # Map rating from [0, 10] to [-max_score * 25%, max_score * 25%] and add it to the ES score]
            rating_factor = rating / 10 * max_score * 0.25
            score = hit['_score'] + rating_factor
            personalized_hits.append({
                'id': hit['_id'],
                'personalized_score': score,
                'rating_factor': rating_factor
            })
        t1 = time()
        print('loop', t1-t0)

        personalized_hits.sort(key=lambda x: x['personalized_score'], reverse=True)
        return personalized_hits[:len(personalized_hits) // 10]  # First 10%


if __name__ == '__main__':
    user_id = int(input('User id: '))
    query = input('Search query: ')

    connection = sqlite3.connect("db.sqlite3")
    books = Search(connection, connection.cursor()).get_books(user_id, query, False)

    print(f'\nShowing {len(books)} books:\n')
    print('ISBN        Personalized score  From rating')
    print('----------  ------------------  -----------')
    for book in books:
        from_rating = f'{book["rating_factor"]:12.2f}' if book['rating_factor'] else '          --'
        print(f'{book["id"]} {book["personalized_score"]:19.2f} {from_rating}')

connection.close()
