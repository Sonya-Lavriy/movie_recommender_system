import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime

import train_test_split
from base_model import CollaborativeFilteringModel, evaluation

users_df = pd.read_csv('data/raw/100k/users.csv')
users_df.drop_duplicates(keep='first', inplace=True)
users_df.drop(['Unnamed: 0'], axis=1, inplace=True)
print(users_df.head())

# engine = create_engine('postgresql://postgres:087539@localhost:5432/RecSys')
# users_df.to_sql('users', engine, if_exists='replace', index=False)

rating_df = pd.read_csv('data/raw/100k/rating.csv')
rating_df.drop_duplicates(keep='first', inplace=True)
rating_df.drop(['Unnamed: 0'], axis=1, inplace=True)
# rating_df.to_sql('ratings', engine, if_exists='replace', index=False)

movies_df = pd.read_csv('data/raw/100k/movies.csv')
movies_df.drop_duplicates(subset=['title'], keep='first', inplace=True)
movies_df.drop([266], axis=0, inplace=True)
movies_df.drop(['Unnamed: 0'], axis=1, inplace=True)
for row in movies_df.index:
    movies_df.at[row, 'title'].replace("'", "`")
print(movies_df.head())
# movies_df.to_sql('movies', engine, if_exists='replace', index=False)


# all_users_avg_rating = rating_df.groupby('user_id')['rating'].mean()
# bool_rating = []
# for row in range(len(rating_df)):
#     user = rating_df.loc[row, 'user_id']
#     rating = rating_df.loc[row, 'rating']
#     if rating >= all_users_avg_rating[user]:
#         bool_rating.append(1)
#     else:
#         bool_rating.append(0)
#
# rating_df['bool_rating'] = bool_rating
# # print(rating_df.loc[55:60])
#
# train_df, test_df = train_test_split.train_test_split(rating_df, 'timestamp', 0.75)
#
# # COLLABORATIVE FILTERING
#
# model = CollaborativeFilteringModel()
# model.fit(train_df)
# # print(model.preference_matrix)
#
# # print(model.predict_one(259, 895))
# print('Test df: ')
# print(test_df.head())
#
# print('\nTest df after prediction:')
# prediction_df = model.predict(test_df.head())
# print(prediction_df.head())
#
# print('\nTop-N prediction: ')
# top_prediction = model.top_n(729)
# top_prediction = pd.merge(top_prediction, movies_df[['item_id', 'title']], how='inner', on='item_id')
# top_prediction = pd.merge(top_prediction,
#                           rating_df[rating_df['user_id'] == 729][['item_id', 'rating', 'bool_rating']],
#                           how='left', on='item_id')
# print(top_prediction)
#
# print('\nAccuracy: ')
# print("\tMAE: {}\n\tRMSE: {}".format(evaluation(prediction_df)[0], evaluation(prediction_df)[1]))
#
# precision = len(top_prediction[top_prediction['bool_rating'] == 1]) / len(top_prediction)
# print('\tPrecision: ', precision)
#
# user_data = rating_df[rating_df['user_id'] == 729]
# recall = len(top_prediction[top_prediction['bool_rating'] == 1]) / len(user_data[user_data['bool_rating'] == 1])
# print('\tRecall: ', recall)
#
# f1 = 2 * precision * recall / (precision + recall)
# print("\tF1: ", f1)
#
#
