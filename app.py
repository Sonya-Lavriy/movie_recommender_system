from flask import Flask, render_template, request, redirect
import psycopg2
import os
import pickle
import pandas as pd
from service import RecommenderService
from base_model import CollaborativeFilteringModel, ContentBasedModel
from annoy import AnnoyIndex


app = Flask(__name__)

service = RecommenderService()

models_path = 'models'
if not os.path.exists(models_path):
    os.makedirs(models_path)
if 'collaborative_filtering.pickle' in os.listdir(models_path):
    print('collaborative_filtering model exists')
    service.cf_model = pickle.load(open(models_path + '/collaborative_filtering.pickle', 'rb'))
else:
    print('collaborative_filtering training model')
    rating_df = pd.read_csv('data/raw/100k/rating.csv')
    rating_df.drop(['Unnamed: 0'], axis=1, inplace=True)
    service.cf_model = CollaborativeFilteringModel()
    service.cf_model.fit(rating_df)
    service.cf_model.save_model()

if 'content_based.pickle' in os.listdir(models_path):
    print('content_based model exists')
    service.cb_model = pickle.load(open(models_path + '/content_based.pickle', 'rb'))
    service.cb_model.index = AnnoyIndex(20, 'angular')
    service.cb_model.index.load(f'models/{service.cb_model.model_name}_index.ann')
else:
    print('content_based training model')
    rating_df = pd.read_csv('data/raw/100k/rating.csv')
    rating_df.drop(['Unnamed: 0'], axis=1, inplace=True)

    movies_df = pd.read_csv('data/raw/100k/movies.csv')
    movies_df.drop([266], axis=0, inplace=True)
    movies_df.drop(['Unnamed: 0'], axis=1, inplace=True)

    service.cb_model = ContentBasedModel()
    service.cb_model.fit(movies_df, rating_df)
    service.cb_model.save_model()


def get_db_connection():
    conn = psycopg2.connect(host='localhost',
                            database='RecSys',
                            user='postgres',
                            password='087539')
    return conn


@app.route('/')
def index():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        "select movies.item_id, movies.title, movies.release_date, movies.imdb_url, count(ratings.user_id) as number_of_ratings from movies inner join ratings on (movies.item_id=ratings.item_id) group by 1, 2, 3, 4 order by 5 desc limit 10")
    most_popular_movies = cur.fetchall()
    cur.execute(
        "select movies.item_id,		movies.title,		movies.release_date, movies.imdb_url, avg(ratings.rating) as avg_rating from movies inner join ratings on (movies.item_id=ratings.item_id) group by 1, 2, 3, 4 having count(ratings.user_id) >= 20 order by 5 desc limit 10")
    most_rated_movies = cur.fetchall()
    cur.close()
    conn.close()
    return render_template("unloged_main_page.html", most_popular_movies=most_popular_movies,
                           most_rated_movies=most_rated_movies)


@app.route('/main/<int:user_id>')
def main(user_id):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        "select movies.item_id, movies.title, movies.release_date, movies.imdb_url, count(ratings.user_id) as number_of_ratings from movies inner join ratings on (movies.item_id=ratings.item_id) group by 1, 2, 3, 4 order by 5 desc limit 10")
    most_popular_movies = cur.fetchall()
    cur.execute(
        "select movies.item_id,		movies.title,		movies.release_date, movies.imdb_url, avg(ratings.rating) as avg_rating from movies inner join ratings on (movies.item_id=ratings.item_id) group by 1, 2, 3, 4 having count(ratings.user_id) >= 20 order by 5 desc limit 10")
    most_rated_movies = cur.fetchall()
    cur.close()
    conn.close()
    return render_template("loged_main_page.html", user_id=user_id, most_popular_movies=most_popular_movies,
                           most_rated_movies=most_rated_movies)


@app.route('/login', methods=['POST', 'GET'])
def login():
    if request.method == 'POST':
        service.uid = int(request.form["id"])

        conn = get_db_connection()
        cur = conn.cursor()
        sql_query = f"SELECT * FROM users where user_id={service.uid}"
        cur.execute(sql_query)
        result = cur.fetchall()
        num_users = len(result)

        if num_users == 0:
            return redirect('/signup')
        else:
            return redirect(f"/main/{service.uid}")

        cur.close()
        conn.close()

    elif request.method == 'GET':
        return render_template("login.html")


@app.route('/signup')
def signup():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM users order by user_id desc limit 1")
    max_id_user = cur.fetchall()
    service.uid = max_id_user[0][0] + 1
    cur.close()
    conn.close()
    return render_template("signup.html", new_id=service.uid)


@app.route('/pers_recs')
def personal_recs():
    cf_recs_df = service.cf_model.top_n(service.uid, 10)
    cf_recs_id = tuple(cf_recs_df['item_id'])
    # print(recs_df.shape)

    cb_recs_df = service.cb_model.top_n(service.uid, 10)
    cb_recs_id = tuple(cb_recs_df['item_id'])
    print(cb_recs_id)

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM movies WHERE item_id in {cf_recs_id}")
    cf_movies_info = pd.DataFrame(cur.fetchall())
    cf_movies_info.columns = ['item_id', 'title', 'release_date', 'imdb_url', 'unknown', 'Action', 'Adventure',
                              'Animation', "Children's", 'Comedy', 'Crime', 'Documentary', 'Drama', 'Fantasy',
                              'Film-Noir', 'Horror', 'Musical', 'Mystery', 'Romance', 'Sci-Fi', 'Thriller', 'War',
                              'Western']
    # print(movies_info.shape)

    cur.execute(f"SELECT * FROM movies WHERE item_id in {cb_recs_id}")
    cb_movies_info = pd.DataFrame(cur.fetchall())
    cb_movies_info.columns = ['item_id', 'title', 'release_date', 'imdb_url', 'unknown', 'Action', 'Adventure',
                              'Animation', "Children's", 'Comedy', 'Crime', 'Documentary', 'Drama', 'Fantasy',
                              'Film-Noir', 'Horror', 'Musical', 'Mystery', 'Romance', 'Sci-Fi', 'Thriller', 'War',
                              'Western']
    print(cb_movies_info)

    cf_top_n_df = cf_recs_df.merge(cf_movies_info, how='inner', on='item_id')
    cb_top_n_df = cb_recs_df.merge(cb_movies_info, how='inner', on='item_id')
    print(cb_top_n_df)

    cur.close()
    conn.close()
    return render_template('personal_recs.html', user_id=service.uid, cf_recs=cf_top_n_df.values.tolist(),
                           cb_recs=cb_top_n_df.values.tolist())


if __name__ == '__main__':
    app.run(debug=True)
