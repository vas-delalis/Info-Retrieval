from kmodes.kprototypes import KPrototypes
from pathlib import Path
import pickle
import sqlite3
import pandas as pd


def train_model(data, cluster_count: int, init_count: int):
    # Train
    print('Training model...')
    model = KPrototypes(n_clusters=cluster_count, n_init=init_count, init='Cao', verbose=1, gamma=500)
    model.fit(data, categorical=[1])
    print(model.cluster_centroids_)

    # Save model
    with open('kprototypes.pickle', 'wb') as f:
        pickle.dump(model, f, pickle.HIGHEST_PROTOCOL)
        print('Saved model as kprototypes.pickle')

    return model


def generate_cluster_ratings(connection: sqlite3.Connection, cursor, retrain=False):
    users = pd.read_sql_query('SELECT * FROM users', con=connection, index_col='id')
    data = users[['age', 'country']].dropna()

    if retrain or not Path('kprototypes.pickle').is_file():
        model = train_model(data, cluster_count=2, init_count=1)
    else:
        with open('kprototypes.pickle', 'rb') as f:
            model = pickle.load(f)

    # Predict
    print('Assigning users to clusters...')
    clusters = model.predict(data, categorical=[1])

    # Pair clusters with respective user ids, then insert into DF (with NaN for missing values)
    users['cluster'] = pd.Series(clusters, data.index).reindex_like(users)

    # Persist cluster membership
    cursor.execute('DELETE FROM users')  # INSERTs can be batched, unlike UPDATEs
    users.to_sql('users', con=connection, if_exists='append')

    # Calculate cluster ratings
    cursor.execute('DELETE FROM cluster_ratings')
    cursor.execute('INSERT INTO cluster_ratings '
                   'SELECT cluster, isbn, AVG(rating) AS avg_rating '
                   'FROM ratings INNER JOIN users on users.id = ratings.user_id '
                   'WHERE cluster IS NOT NULL '
                   'GROUP BY isbn, cluster')
    connection.commit()


if __name__ == '__main__':
    connection = sqlite3.connect("db.sqlite3")
    cursor = connection.cursor()

    generate_cluster_ratings(connection, cursor, retrain=False)

    connection.close()
