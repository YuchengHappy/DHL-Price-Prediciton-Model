import pandas as pd
import tensorflow as tf
from tensorflow import keras
from pathlib import Path
import matplotlib.pyplot as plt
from sklearn.metrics import r2_score
from data_processing import data_processing
from sklearn.model_selection import RepeatedKFold


def read_file(file_path):
    return pd.read_csv(file_path, low_memory=False)


def r2(actual, predicted):
    """ R2 Score """
    return r2_score(actual, predicted)


def adjr2(actual, predicted, rowcount, featurecount):
    """ R2 Score """
    return 1 - (1 - r2(actual, predicted)) * (rowcount - 1) / (rowcount - featurecount)


# TODO:implement model here



def k_fold_train(df, model, n_fold, _n_repeats):
    r_sq = []
    column_name = list(df.columns)
    column_name = [x for x in column_name if x not in ["TOTAL_ACTUAL_COST", "LINEHAUL_COSTS", "FUEL_COSTS", "ACC._COSTS"]]
    y = df["TOTAL_ACTUAL_COST"]
    y = y.to_numpy()
    X = df.drop(columns=["TOTAL_ACTUAL_COST", "LINEHAUL_COSTS", "FUEL_COSTS", "ACC._COSTS"]).to_numpy()
    random_state = 7749
    rkf = RepeatedKFold(n_splits=n_fold, n_repeats=_n_repeats, random_state=random_state)
    for train_index, test_index in rkf.split(df):
        X_train, X_test = X[train_index], X[test_index]
        y_train, y_test = y[train_index], y[test_index]

        X_train, X_test, y_train, y_test = data_processing(X_train, X_test, y_train, y_test, column_name)

        inputs = tf.keras.layers.Input(shape=(35,))
        layer_1 = tf.keras.layers.Dense(35*30, activation=tf.nn.relu)(inputs)
        layer_2 = tf.keras.layers.Dense(50, activation=tf.nn.relu)(layer_1)
        outputs = tf.keras.layers.Dense(1)(layer_2)
        model = tf.keras.models.Model(inputs=inputs, outputs=outputs)

        model.compile(optimizer="Adam", loss="mse", metrics=["mse", "mae"])

        # fit the model
        model.fit(X_train, y_train, epochs=20)

        # Prediction
        y_predict = model.predict(X_test)

        # TODO: print performance
        r_sq.append(r2(y_test, y_predict))

    print(sum(r_sq)/len(r_sq))




k_fold_train(read_file("/Users/luyucheng/Desktop/APAN 5900/data/FE_DONE.csv"), 1, 5, 1)
