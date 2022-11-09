import pandas as pd
from datetime import timedelta
from datetime import datetime


# input type: string
def read_file(file_path):
    df = pd.read_csv(file_path, low_memory=False)

    # return type: pandas dataframe
    yield df


# input type: pandas dataframe
def remove_zero_null(df):
    # drop columns "PU_ARRIVAL_(X3)", "PU_DEPARTED_(AF)", "DL_ARRIVAL_(X1)", "DL_DEPARTED_(D1)"
    df = df.drop(columns=["PU_ARRIVAL_(X3)", "PU_DEPARTED_(AF)", "DL_ARRIVAL_(X1)",
                          "DL_DEPARTED_(D1)"])

    # Drop negative values
    df = df[df["CASES"] >= 0]
    df = df[df["LINEHAUL_COSTS"] >= 0]
    df = df[df["FUEL_COSTS"] >= 0]
    df = df[df["ACC._COSTS"] >= 0]
    df = df[df["TOTAL_ACTUAL_COST"] >= 0]

    # Drop 0s and nulls Total_Actual_Cost
    df = df[df["TOTAL_ACTUAL_COST"] != 0]
    df = df.dropna(subset=["TOTAL_ACTUAL_COST"])

    # Drop 0s, negatives, and nulls DISTANCE
    df = df[df["DISTANCE"] > 0]
    df = df.dropna(subset=["DISTANCE"])

    # Drop 0s and nulls VOLUME
    df = df[df["VOLUME"] > 0]
    df = df.dropna(subset=["VOLUME"])

    # Drop 0s and nulls WEIGHT
    df = df[df["WEIGHT"] > 0]
    df = df.dropna(subset=["WEIGHT"])

    # drop nulls in Zip, STATE, and CITY
    df = df.dropna(subset=["ORIGIN_ZIP", "DEST_ZIP", "ORIGIN_STATE", "DEST_STATE",
                           "ORIGIN_CITY", "DEST_CITY"])

    # Drop Null in PU_APPT and DL_APPT
    df = df.dropna(subset=["PU_APPT", "DL_APPT"])

    # keep last row
    df = df.sort_values("Insert_Date").groupby("SHIPMENT_ID").tail(1)

    # output type: pandas dataframe
    yield df


# input type: pandas dataframe
def mode_clean(df):
    # Add "." in front of all entries for ACTUAL_MODE in order to split the string. Some entries are "LTL", "TL"
    df["ACTUAL_MODE"] = "." + df["ACTUAL_MODE"].astype(str)

    # Split string and keep the last element of string
    df["ACTUAL_MODE"] = df["ACTUAL_MODE"].str.split(".").str.get(-1)

    # Only keep entries with "LTL", "TL", and "INTERMODAL" in ACTUAL_MODE
    mode_list = ["LTL", "TL", "INTERMODAL"]
    df = df[df["ACTUAL_MODE"].isin(mode_list)]

    # output type: pandas dataframe
    yield df


# input type: pandas dataframe
def state_clean(df):
    # TODO

    # output type: pandas dataframe
    yield df


# input type: pandas dataframe
def appointment_clean(df):

    df["PU_APPT"] = df["PU_APPT"] + " "
    df["DL_APPT"] = df["DL_APPT"] + " "

    df["PU_APPT_DATETIME"] = df["PU_APPT"].str.split(" ").str.get(0) + " " + df["PU_APPT"].str.split(" ").str.get(1)
    df["PU_APPT_TIMEZONE"] = df["PU_APPT"].str.split(" ").str.get(2)

    df["DL_APPT_DATETIME"] = df["DL_APPT"].str.split(" ").str.get(0) + " " + df["DL_APPT"].str.split(" ").str.get(1)
    df["DL_APPT_TIMEZONE"] = df["DL_APPT"].str.split(" ").str.get(2)

    df["PU_APPT_DATETIME"] = pd.to_datetime(df["PU_APPT_DATETIME"], errors='coerce')
    df["DL_APPT_DATETIME"] = pd.to_datetime(df["DL_APPT_DATETIME"], errors='coerce')

    df["PU_APPT_TIMEZONE"][df["PU_APPT_TIMEZONE"] != 'AMERICA/CHICAGO'] = datetime.timedelta(hours=0)
    df["PU_APPT_TIMEZONE"][df["PU_APPT_TIMEZONE"] == 'AMERICA/CHICAGO'] = datetime.timedelta(hours=1)

    df["DL_APPT_TIMEZONE"][df["DL_APPT_TIMEZONE"] != 'AMERICA/CHICAGO'] = datetime.timedelta(hours=0)
    df["DL_APPT_TIMEZONE"][df["DL_APPT_TIMEZONE"] == 'AMERICA/CHICAGO'] = datetime.timedelta(hours=1)

    df["PU_APPT_DATETIME"] = df["PU_APPT_DATETIME"] + df["PU_APPT_TIMEZONE"]
    df["DL_APPT_DATETIME"] = df["DL_APPT_DATETIME"] + df["DL_APPT_TIMEZONE"]

    df = df.dropna(subset=["PU_APPT_DATETIME", "DL_APPT_DATETIME"])

    df = df.drop(columns=["DL_APPT_TIMEZONE", "PU_APPT_TIMEZONE", "PU_APPT", "DL_APPT"])

    # output type: pandas dataframe
    yield df
