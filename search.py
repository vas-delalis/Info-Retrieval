from elasticsearch_client import client
import sqlite3

connection = sqlite3.connect("db.sqlite3")
cursor = connection.cursor()


def get_ratings(user_id: int):
    cursor.execute('SELECT isbn, rating FROM user_ratings WHERE user_id = ?', [user_id])
    user_ratings = cursor.fetchall()
    return dict(user_ratings)


def get_books(query: str, user_ratings):
    # Search
    result = client.search(index="books", query={'multi_match': {'query': query, 'fields': ['title^2', 'summary']}},
                           source=False, size=10000)
    max_score = result['hits']['max_score']
    hits = result['hits']['hits']

    # Process
    personalized_hits = []
    for hit in hits:
        # Map rating from [0, 10] to [-max_score * 25%, max_score * 25%] and add it to the ES score
        rating = user_ratings[hit['_id']] if hit['_id'] in user_ratings else 0
        rating_factor = rating / 10 * max_score * 0.25
        score = hit['_score'] + rating_factor
        personalized_hits.append({
            'id': hit['_id'],
            'personalized_score': score,
            'rating_factor': rating_factor
        })

    personalized_hits.sort(key=lambda x: x['personalized_score'], reverse=True)
    return personalized_hits[:len(personalized_hits) // 10]  # First 10%


if __name__ == '__main__':
    user_id = int(input('User id: '))
    query = input('Search query: ')

    books = get_books(query, get_ratings(user_id))

    print(f'\nShowing {len(books)} books:\n')
    print('ISBN        Personalized score  From rating')
    print('----------  ------------------  -----------')
    for book in books:
        from_rating = f'{book["rating_factor"]:12.2f}' if book['rating_factor'] else '          --'
        print(f'{book["id"]} {book["personalized_score"]:19.2f} {from_rating}')

connection.close()
