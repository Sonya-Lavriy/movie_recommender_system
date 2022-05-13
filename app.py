from flask import Flask, render_template, request, redirect
import psycopg2
import os
import pickle
import pandas as pd
from datetime import datetime
import time
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
    movies_df.drop_duplicates(subset=['title'], keep='first')
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


@app.route('/', methods=['POST', 'GET'])
def index():
    if request.method == 'GET':
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(
            "select full_movies.item_id, full_movies.title, full_movies.release_date, full_movies.imdb_url_new, full_movies.poster_url, count(ratings.user_id) as number_of_ratings from full_movies inner join ratings on (full_movies.item_id=ratings.item_id) group by 1, 2, 3, 4, 5 order by 6 desc limit 10")
        most_popular_movies = cur.fetchall()
        cur.execute(
            "select full_movies.item_id, full_movies.title,	full_movies.release_date, full_movies.imdb_url_new, avg(ratings.rating) as avg_rating from full_movies inner join ratings on (full_movies.item_id=ratings.item_id) group by 1, 2, 3, 4 having count(ratings.user_id) >= 20 order by 5 desc limit 10")
        most_rated_movies = cur.fetchall()
        cur.close()
        conn.close()
        return render_template("unloged_main_page.html", most_popular_movies=most_popular_movies,
                               most_rated_movies=most_rated_movies)
    else:
        if request.form['btn'] == 'Search':
            service.keyword = request.form['search_movie']
            return redirect('/unloged_search_result')


@app.route('/main', methods=['POST', 'GET'])
def main():
    if request.method == 'GET':
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(
            "select full_movies.item_id, full_movies.title, full_movies.release_date, full_movies.imdb_url_new, count(ratings.user_id) as number_of_ratings from full_movies inner join ratings on (full_movies.item_id=ratings.item_id) group by 1, 2, 3, 4 order by 5 desc limit 12")
        most_popular_movies = cur.fetchall()
        cur.execute(
            "select full_movies.item_id, full_movies.title,	full_movies.release_date, full_movies.imdb_url_new, avg(ratings.rating) as avg_rating from full_movies inner join ratings on (full_movies.item_id=ratings.item_id) group by 1, 2, 3, 4 having count(ratings.user_id) >= 20 order by 5 desc limit 12")
        most_rated_movies = cur.fetchall()
        cur.close()
        conn.close()
        return render_template("loged_main_page.html", user_id=service.uid, most_popular_movies=most_popular_movies,
                               most_rated_movies=most_rated_movies)
    elif request.method == 'POST':
        if request.form['btn'] == 'Add to wishlist':
            item_id = request.form['id']
            print(item_id)
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute(
                f'INSERT INTO wishlist (user_id, item_id, datetime, from_recommendations) VALUES ({service.uid}, {item_id}, current_date, TRUE )')
            conn.commit()
            count = cur.rowcount
            print(count, "Record inserted successfully into wishlist table")
            cur.close()
            conn.close()
            return redirect(request.url)
        elif request.form['btn'] == 'Search':
            service.keyword = request.form['search_movie']
            return redirect('/search_result')
        elif request.form['btn'] == 'Rate':
            print('Film is rated')
            rating = int(request.form['inlineRadioOptions'])
            print('rating is:', rating)
            item_id = request.form['id']
            print('rated was item with id: ', item_id)
            timestamp = int(time.mktime(datetime.now().timetuple()))
            print('rated timestamp: ', timestamp)
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute(
                f'INSERT INTO ratings (user_id, item_id, rating, timestamp) VALUES ({service.uid}, {item_id}, {rating}, {timestamp} )')
            conn.commit()
            count = cur.rowcount
            print(count, "record inserted successfully into rating table")
            cur.close()
            conn.close()
            return redirect(request.url)


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
            return redirect('/main')

        cur.close()
        conn.close()

    elif request.method == 'GET':
        return render_template("login.html")


@app.route('/signup', methods=['POST', 'GET'])
def signup():
    if request.method == 'GET':
        print('You open page')
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT * FROM users order by user_id desc limit 1")
        max_id_user = cur.fetchall()
        service.uid = max_id_user[0][0] + 1
        cur.close()
        conn.close()
        return render_template("signup.html", new_id=service.uid)
    elif request.method == 'POST':
        print('You send form')
        age = int(request.form['age'])
        print(age)
        sex = request.form.get('sex')
        print(sex)
        occupation = request.form.get('occupation')
        print(occupation)
        zip_code = request.form.get('zip_code')
        print(zip_code)

        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT * FROM users order by user_id desc limit 1")
        max_id_user = cur.fetchall()
        service.uid = max_id_user[0][0] + 1
        cur.execute(
            f"INSERT INTO users (user_id, age, gender, occupation, zip_code) VALUES ({service.uid}, {age}, '{sex}', '{occupation}', '{zip_code}')")
        conn.commit()
        cur.close()
        conn.close()
        print('User successfully added')
        return redirect('/main')


@app.route('/pers_recs', methods=['POST', 'GET'])
def personal_recs():
    if request.method == 'GET':
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(f'SELECT * FROM ratings where user_id={service.uid}')
        res = cur.fetchall()
        if len(res) == 0:
            return redirect('/unfound_pers_recs')
        else:
            cf_recs_df = service.cf_model.top_n(service.uid, 10)
            cf_recs_id = tuple(cf_recs_df['item_id'])
            # print(recs_df.shape)

            cb_recs_df = service.cb_model.top_n(service.uid, 10)
            cb_recs_id = tuple(cb_recs_df['item_id'])
            print(cb_recs_id)

            cur.execute(
                f"SELECT item_id, title, release_date, imdb_url_new, poster_url FROM full_movies WHERE item_id in {cf_recs_id}")
            cf_movies_info = pd.DataFrame(cur.fetchall())
            cf_movies_info.columns = ['item_id', 'title', 'release_date', 'imdb_url', 'poster_url']
            # print(movies_info.shape)

            cur.execute(
                f"SELECT item_id, title, release_date, imdb_url_new, poster_url FROM full_movies WHERE item_id in {cb_recs_id}")
            cb_movies_info = pd.DataFrame(cur.fetchall())
            cb_movies_info.columns = ['item_id', 'title', 'release_date', 'imdb_url', 'poster_url']
            print(cb_movies_info)

            cf_top_n_df = cf_recs_df.merge(cf_movies_info, how='inner', on='item_id')
            cb_top_n_df = cb_recs_df.merge(cb_movies_info, how='inner', on='item_id')
            print(cb_top_n_df)

            cur.close()
            conn.close()
            return render_template('personal_recs.html', user_id=service.uid, cf_recs=cf_top_n_df.values.tolist(),
                                   cb_recs=cb_top_n_df.values.tolist())
    elif request.method == 'POST':
        if request.form['btn'] == 'Add to wishlist':
            item_id = request.form['id']
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute(
                f'INSERT INTO wishlist (user_id, item_id, datetime, from_recommendations) VALUES ({service.uid}, {item_id}, current_date, TRUE )')
            conn.commit()
            count = cur.rowcount
            print(count, "Record inserted successfully into wishlist table")
            cur.close()
            conn.close()
            return redirect(request.url)
        elif request.form['btn'] == 'Rate':
            rating = int(request.form['inlineRadioOptions'])
            print('rating is:', rating)
            item_id = request.form['id']
            print('rated was item with id: ', item_id)
            timestamp = int(time.mktime(datetime.now().timetuple()))
            print('rated timestamp: ', timestamp)
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute(
                f'INSERT INTO ratings (user_id, item_id, rating, timestamp) VALUES ({service.uid}, {item_id}, {rating}, {timestamp} )')
            conn.commit()
            count = cur.rowcount
            print(count, "record inserted successfully into rating table")
            cur.close()
            conn.close()
            return redirect(request.url)
        elif request.form['btn'] == 'Search':
            service.keyword = request.form['search_movie']
            return redirect('/search_result')


@app.route('/rated_films', methods=['POST', 'GET'])
def rated_films():
    if request.method == 'GET':
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(
            f"SELECT distinct ratings.item_id, title, release_date, imdb_url_new, poster_url, rating, ratings.timestamp FROM ratings left join full_movies on (ratings.item_id=full_movies.item_id) WHERE ratings.user_id = {service.uid} order by ratings.timestamp desc")
        result = cur.fetchall()
        cur.close()
        conn.close()
        return render_template('rated_films.html', user_id=service.uid, rated_films=result)
    else:
        if request.form['btn'] == 'Search':
            service.keyword = request.form['search_movie']
            return redirect('/search_result')


@app.route('/wishlist', methods=['POST', 'GET'])
def wishlist():
    if request.method == 'GET':
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(
            f'SELECT distinct wishlist.item_id, title, release_date, imdb_url_new, datetime FROM wishlist inner join full_movies on (wishlist.item_id=full_movies.item_id) WHERE user_id={service.uid} ORDER BY datetime desc')
        result = cur.fetchall()
        cur.close()
        conn.close()
        return render_template('wishlist.html', user_id=service.uid, wishlist=result)
    else:
        if request.form['btn'] == 'Delete':
            print('Delete request')
            movie_id = request.form['id']
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute(
                f'DELETE FROM wishlist WHERE user_id={service.uid} AND item_id={movie_id}')
            conn.commit()
            print('Row deleted')
            cur.close()
            conn.close()
            return redirect(request.url)
        elif request.form['btn'] == 'Search':
            service.keyword = request.form['search_movie']
            return redirect('/search_result')
        elif request.form['btn'] == 'Rate':
            rating = int(request.form['inlineRadioOptions'])
            print('rating is:', rating)
            item_id = request.form['id']
            print('rated was item with id: ', item_id)
            timestamp = int(time.mktime(datetime.now().timetuple()))
            print('rated timestamp: ', timestamp)
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute(
                f'INSERT INTO ratings (user_id, item_id, rating, timestamp) VALUES ({service.uid}, {item_id}, {rating}, {timestamp} )')
            conn.commit()
            cur.execute(f'DELETE FROM wishlist WHERE user_id={service.uid} AND item_id={item_id}')
            conn.commit()
            count = cur.rowcount
            print(count, "record inserted successfully into rating table")
            cur.close()
            conn.close()
            return redirect(request.url)


@app.route('/search_result', methods=['POST', 'GET'])
def search():
    if request.method == 'GET':
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(
            f"SELECT item_id, title, release_date, imdb_url_new FROM full_movies WHERE lower(title) like '%{service.keyword}%'")
        result = cur.fetchall()
        cur.close()
        conn.close()
        return render_template('loged_search_result.html', user_id=service.uid, films=result)
    else:
        if request.form['btn'] == 'Add to wishlist':
            item_id = request.form['id']
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute(
                f'INSERT INTO wishlist (user_id, item_id, datetime, from_recommendations) VALUES ({service.uid}, {item_id}, current_date, FALSE )')
            conn.commit()
            count = cur.rowcount
            print(count, "Record inserted successfully into wishlist table")
            cur.close()
            conn.close()
            return redirect(request.url)
        elif request.form['btn'] == 'Rate':
            rating = int(request.form['inlineRadioOptions'])
            print('rating is:', rating)
            item_id = request.form['id']
            print('rated was item with id: ', item_id)
            timestamp = int(time.mktime(datetime.now().timetuple()))
            print('rated timestamp: ', timestamp)
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute(
                f'INSERT INTO ratings (user_id, item_id, rating, timestamp) VALUES ({service.uid}, {item_id}, {rating}, {timestamp} )')
            conn.commit()
            count = cur.rowcount
            print(count, "record inserted successfully into rating table")
            cur.close()
            conn.close()
            return redirect(request.url)

@app.route('/unloged_search_result')
def unloged_search():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        f"SELECT item_id, title, release_date, imdb_url_new FROM full_movies WHERE lower(title) like '%{service.keyword}%'")
    result = cur.fetchall()
    cur.close()
    conn.close()
    return render_template('unloged_search_result.html', films=result)


@app.route('/unfound_pers_recs')
def unfound_pers_recs():
    return render_template('unfound_pers_recs.html', user_id=service.uid)


if __name__ == '__main__':
    app.run(debug=True)
