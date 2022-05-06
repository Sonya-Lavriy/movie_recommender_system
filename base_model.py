from copy import deepcopy

import pandas as pd
import numpy as np
import pickle
from datetime import datetime
from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.decomposition import NMF
from annoy import AnnoyIndex


def evaluation(prediction_df):
    prediction_df_copy = deepcopy(prediction_df)
    prediction_df_copy['abs_delta'] = abs(prediction_df['rating'] - prediction_df['predicted_rating'])
    prediction_df_copy['squared_delta'] = (prediction_df['rating'] - prediction_df['predicted_rating']) ** 2
    mae = prediction_df_copy.groupby('user_id')['abs_delta'].mean().mean()
    mse = prediction_df_copy.groupby('user_id')['squared_delta'].mean().mean()
    rmse = np.sqrt(mse)
    return mae, rmse


class BaseModel:
    def __init__(self):
        self.model_name = None

    def fit(self):
        pass

    def predict(self):
        pass

    def save_model(self):
        pickle.dump(self, open(f'models/{self.model_name}.pickle', 'wb'))


class CollaborativeFilteringModel(BaseModel):
    # TODO: limit to predicted rating: where to force it?
    def __init__(self):
        self.model_name = 'collaborative_filtering'
        self.rating_matrix = None
        self.mean_users_rating = None
        self.preference_matrix = None
        self.cosine_matrix = None
        self.users_distances = None

    def fit(self, train_df):
        self.rating_matrix = pd.pivot_table(train_df, values='rating', index='user_id', columns=['item_id'])
        self.mean_users_rating = self.rating_matrix.mean(axis=1)
        self.preference_matrix = self.rating_matrix.subtract(self.mean_users_rating, axis=0).fillna(0)
        self.cosine_matrix = cosine_similarity(self.preference_matrix)
        self.users_distances = pd.DataFrame(self.cosine_matrix,
                                            index=self.rating_matrix.index,
                                            columns=self.rating_matrix.index)

    def predict_one(self, user, film, neighbours=10, threshold=0.15):
        # 1) берем тих, хто дивився фільм
        # 2) з них вибираємо найближчих
        # 3) лишаємо тих, хто по близькості перевищує поріг
        # 4) знаходимо ваги для кожного сусіда
        # 5) обчислюємо прогнозований рейтинг

        if user in self.rating_matrix.index and film in self.rating_matrix.columns:
            users_who_saw_film = self.rating_matrix.loc[:, film].notnull()
            user_distances = self.users_distances.loc[users_who_saw_film].loc[:, user]
            filtered_distances = user_distances[user_distances > threshold].sort_values(ascending=False).head(
                neighbours)
            users_weights = filtered_distances / sum(filtered_distances)
            users_preferences = self.preference_matrix.loc[users_weights.index, film]
            user_delta = users_weights.dot(users_preferences)
            rating_prediction = self.mean_users_rating.loc[user] + user_delta
            # print('all info known')
        elif film in self.rating_matrix.columns:
            rating_prediction = self.rating_matrix.loc[:, film].mean()
            # print('user unknown')
        else:
            rating_prediction = self.mean_users_rating.loc[user]
            # print('film unknown')

        return rating_prediction

    def predict(self, test_df):
        prediction_list = []
        # TODO optimize: change row-wise to df
        for user, film in zip(test_df['user_id'], test_df['item_id']):
            prediction_list.append(self.predict_one(user, film))

        test_df_copy = deepcopy(test_df)
        test_df_copy['predicted_rating'] = prediction_list
        return test_df_copy

    def top_n(self, user, n=10):
        # todo exclude films, which user have already rated
        df = pd.DataFrame({'user_id': [user] * len(self.rating_matrix.columns),
                           'item_id': self.rating_matrix.columns})
        prediction = self.predict(df) \
            .sort_values('predicted_rating', ascending=False) \
            .drop(['user_id'], axis=1) \
            .head(n)
        return prediction

    def accuracy(self):
        pass


class ContentBasedModel(BaseModel):
    def __init__(self):
        self.model_name = 'content_based'
        self.rating_df = None
        self.encoded_movies = None
        self.index = None

    def fit(self, movies_df, rating_df):
        self.rating_df = rating_df
        encoded_movies_df = pd.concat([movies_df.iloc[:, 0], pd.to_datetime(movies_df.iloc[:, 2], format='%d-%b-%Y'),
                                       movies_df.iloc[:, 4:].astype(int)], axis=1)
        encoded_movies_df['release_year'] = pd.DatetimeIndex(encoded_movies_df['release_date']).year.to_list()
        encoded_movies_df.drop(['release_date'], axis=1, inplace=True)

        scaler = MinMaxScaler()
        encoded_movies_df['release_year'] = scaler.fit_transform(
            np.array(encoded_movies_df['release_year']).reshape(-1, 1))

        encoded_movies_df = encoded_movies_df.set_index('item_id')

        self.encoded_movies = encoded_movies_df

        f = 20  # Length of item vector that will be indexed

        self.index = AnnoyIndex(f, 'angular')
        for i in encoded_movies_df.index:
            v = encoded_movies_df.loc[i].tolist()
            self.index.add_item(i, v)

        self.index.build(10)  # 10 trees

    def top_n(self, user_id, n=10):
        watched_movies = self.encoded_movies.merge(
            self.rating_df[self.rating_df.user_id == user_id][['item_id', 'rating']],
            how='inner', on='item_id')
        weights = watched_movies['rating'] / watched_movies.rating.sum()
        watched_movies.drop(['rating', 'item_id'], axis=1, inplace=True)
        mean_user_vector = watched_movies.mul(pd.Series(weights), axis=0).mean().to_list()
        top_n = pd.DataFrame(
            {'item_id': self.index.get_nns_by_vector(mean_user_vector, 10, include_distances=True)[0],
             'distance': self.index.get_nns_by_vector(mean_user_vector, 10, include_distances=True)[1]})
        return top_n

    def save_model(self):
        self.index.save(f'models/{self.model_name}_index.ann')
        self.index = None
        super().save_model()


class MatrixFactorizationModel(BaseModel):
    def __init__(self):
        self.model_name = 'matrix_factorization'
        self.train_rating_matrix = None
        self.test_rating_matrix = None
        self.prediction_matrix = None
        self.train_mean_users_rating = None
        self.test_mean_users_rating = None
        self.model = None

    def fit(self, train_df, method_to_fill_na='zeros', num_latent_features=2):
        self.train_rating_matrix = pd.pivot_table(train_df, values='rating', index='user_id', columns=['item_id'])
        self.train_mean_users_rating = self.train_rating_matrix.mean(axis=1)

        if method_to_fill_na == 'zeros':
            self.train_rating_matrix.fillna(0, inplace=True)
        elif method_to_fill_na == 'average':
            for row in self.train_rating_matrix.index:
                self.train_rating_matrix.loc[row].fillna(self.train_mean_users_rating[row], axis=0, inplace=True)
        else:
            print('No such method: only zeros or average')

        self.model = NMF(n_components=num_latent_features, init='nndsvda', random_state=0)

        self.model.fit(self.train_rating_matrix)

    def predict(self, test_df):
        # delete new users and films
        user_filter = test_df['user_id'].isin(self.train_rating_matrix.index)
        item_filter = test_df['item_id'].isin(self.train_rating_matrix.columns)
        test_df_without_new = test_df[user_filter & item_filter]
        # data preparation
        self.test_rating_matrix = pd.pivot_table(test_df_without_new, values='rating',
                                                 index='user_id', columns=['item_id'])
        self.test_mean_users_rating = self.test_rating_matrix.mean(axis=1)
        for row in self.test_rating_matrix.index:
            self.test_rating_matrix.loc[row].fillna(self.test_mean_users_rating[row], axis=0, inplace=True)
        #
        users = self.model.fit_transform(self.test_rating_matrix)
        movies = self.model.components_
        self.prediction_matrix = np.dot(users, movies)

        # if user in self.rating_matrix.index and film in self.rating_matrix.columns:
        #
        #     # print('all info known')
        # elif film in self.rating_matrix.columns:
        #     rating_prediction = self.rating_matrix.loc[:, film].mean()
        #     # print('user unknown')
        # else:
        #     rating_prediction = self.mean_users_rating.loc[user]
        #     # print('film unknown')

        # return rating_prediction
