def train_test_split(df, datetime_column, train_part):
    df = df.sort_values(datetime_column).reset_index(drop=True)
    datetime_dict = df[datetime_column].value_counts().sort_index()
    split_datetime = df.loc[0, datetime_column]

    values_sum = 0
    for key, value in datetime_dict.items():
        if values_sum < len(df) * train_part:
            split_datetime = key
            values_sum += value

    train_df = df[df[datetime_column] < split_datetime].reset_index(drop=True)
    test_df = df[df[datetime_column] >= split_datetime].reset_index(drop=True)
    return train_df, test_df
