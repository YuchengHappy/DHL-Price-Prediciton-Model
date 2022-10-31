import pandas as pd


def add_duration(df):
    df["DURATION"] = df["DL_APPT_DATETIME"] - df["PU_APPT_DATETIME"]

    # drop negative duration
    df = df[df["DURATION"] > pd.Timedelta(0)]

    # add duration days and minute
    df["DURATION_DAY"] = int(df["DURATION"].days)
    df["DURATION_MINUTE"] = int(df["DURATION"].seconds) / 60

    # return dataframe
    yield df
