import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime


def read_file(file_path):

    df = pd.read_csv(file_path, low_memory=False)

    yield df


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

    yield df


def state_clean(df):
    # TODO
    yield df


def add_country(df):

    us_states = ['AK', 'AL', 'AR', 'AZ', 'CA', 'CO', 'CT', 'DC', 'DE', 'FL', 'GA',
                 'HI', 'IA', 'ID', 'IL', 'IN', 'KS', 'KY', 'LA', 'MA', 'MD', 'ME',
                 'MI', 'MN', 'MO', 'MS', 'MT', 'NC', 'ND', 'NE', 'NH', 'NJ', 'NM',
                 'NV', 'NY', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX',
                 'UT', 'VA', 'VT', 'WA', 'WI', 'WV', 'WY']
    canada_states = ['NL', 'PE', 'NS', 'NB', 'QC', 'ON', 'MB',
                     'SK', 'AB', 'BC', 'YT', 'NT', 'NU']

    df_us = df["ORIGIN_STATE"].isin(us_states)
    df_can = df["ORIGIN_STATE"].isin(canada_states)
    df_us = df_us.replace(
        to_replace=[True],
        value="us")
    df_us = df_us.replace(
        to_replace=[False],
        value="")
    df_can = df_can.replace(
        to_replace=[True],
        value="ca")
    df_can = df_can.replace(
        to_replace=[False],
        value="")

    df_origin_con = df_us + df_can

    df_us = df["DEST_STATE"].isin(us_states)
    df_can = df["DEST_STATE"].isin(canada_states)
    df_us = df_us.replace(
        to_replace=[True],
        value="us")
    df_us = df_us.replace(
        to_replace=[False],
        value="")
    df_can = df_can.replace(
        to_replace=[True],
        value="ca")
    df_can = df_can.replace(
        to_replace=[False],
        value="")

    df_dest_con = df_us + df_can

    df["ORIGIN_COUNTRY"] = df_origin_con
    df["DEST_COUNTRY"] = df_dest_con

    # Drop 0s and nulls ORIGIN_COUNTRY and DEST_COUNTRY
    df = df[df["ORIGIN_COUNTRY"] != ""]
    df = df[df["DEST_COUNTRY"] != ""]

    yield df


def appointment_clean(df):

    # Decode PU_APPT and DL_APPT
    df["PU_APPT_DATETIME"] = df["PU_APPT"].str[:16]
    df["DL_APPT_DATETIME"] = df["DL_APPT"].str[:16]
    df["PU_APPT_ZONE"] = df["PU_APPT"].str[16:]
    df["DL_APPT_ZONE"] = df["DL_APPT"].str[16:]

    # Clean
    df["PU_APPT_ZONE"] = df["PU_APPT_ZONE"].replace(
        to_replace=[":00", "", " ET"]
        , value="ET")
    df["DL_APPT_ZONE"] = df["DL_APPT_ZONE"].replace(
        to_replace=[":00", ""]
        , value="ET")
    df["PU_APPT_ZONE"] = df["PU_APPT_ZONE"].replace(
        to_replace=[" AMERICA/CHICAGO"]
        , value="CDT")
    df["DL_APPT_ZONE"] = df["DL_APPT_ZONE"].replace(
        to_replace=[" AMERICA/CHICAGO"]
        , value="CDT")

    next_year = str(datetime.now().year) + 1
    base = "20"

    # Further Cleaning
    df = df[df["PU_APPT_DATETIME"] < next_year]
    df = df[df["DL_APPT_DATETIME"] < next_year]
    df = df[df["PU_APPT_DATETIME"] > base]
    df = df[df["DL_APPT_DATETIME"] > base]
    df = df[df["PU_APPT_DATETIME"].str[4] == "-"]
    df = df[df["PU_APPT_DATETIME"].str[7] == "-"]
    df = df[df["PU_APPT_DATETIME"].str[10] == " "]
    df = df[df["DL_APPT_DATETIME"].str[4] == "-"]
    df = df[df["DL_APPT_DATETIME"].str[7] == "-"]
    df = df[df["DL_APPT_DATETIME"].str[10] == " "]

    # Type to datetime
    df["PU_APPT_DATETIME"] = pd.to_datetime(df["PU_APPT_DATETIME"])
    df["DL_APPT_DATETIME"] = pd.to_datetime(df["DL_APPT_DATETIME"])

    df["PU_APPT_ZONE"] = df["PU_APPT_ZONE"].replace(
        to_replace=["CDT"]
        , value=timedelta(hours=1))
    df["DL_APPT_ZONE"] = df["DL_APPT_ZONE"].replace(
        to_replace=["CDT"]
        , value=timedelta(hours=1))
    df["PU_APPT_ZONE"] = df["PU_APPT_ZONE"].replace(
        to_replace=["ET"]
        , value=timedelta(hours=0))
    df["DL_APPT_ZONE"] = df["DL_APPT_ZONE"].replace(
        to_replace=["ET"]
        , value=timedelta(hours=0))

    df["PU_APPT_DATETIME"] = df["PU_APPT_DATETIME"] + df["PU_APPT_ZONE"]
    df["DL_APPT_DATETIME"] = df["DL_APPT_DATETIME"] + df["DL_APPT_ZONE"]

    # drop columns "INSERT_DATE", "PU_APPT", and "DL_APPT"
    df = df.drop(columns=["Insert_Date", "PU_APPT", "DL_APPT", "DL_APPT_ZONE", "PU_APPT_ZONE"])

    yield df


def add_duration(df):

    df["DURATION"] = df["DL_APPT_DATETIME"] - df["PU_APPT_DATETIME"]

    # drop negative duration
    df = df[df["DURATION"] > pd.Timedelta(0)]

    # TODO: DURATION DAY, MINUTE

    # return dataframe
    yield df


def zip_clean(df):
    # add "0" for 4 digit zip
    df.DEST_ZIP = df.DEST_ZIP.apply(lambda x: x if len(x) != 4 else "0" + x)
    df.ORIGIN_ZIP = df.ORIGIN_ZIP.apply(lambda x: x if len(x) != 4 else "0" + x)

    # change 3 digit zip to null
    df.DEST_ZIP = df.DEST_ZIP.apply(lambda x: x if len(x) >= 5 else np.nan)
    df.ORIGIN_ZIP = df.ORIGIN_ZIP.apply(lambda x: x if len(x) >= 5 else np.nan)

    # XXXXX-XXXX to XXXXX
    df.ORIGIN_ZIP = df.ORIGIN_ZIP.apply(lambda x: x if len(x) != 10 else x[:5])
    df.DEST_ZIP = df.DEST_ZIP.apply(lambda x: x if len(x) != 10 else x[:5])

    # Split CA zip
    df.DEST_ZIP = df.DEST_ZIP.apply(lambda x: x if len(x) != 6 else x[:3] + " " + x[3:])
    df.ORIGIN_ZIP = df.ORIGIN_ZIP.apply(lambda x: x if len(x) != 6 else x[:3] + " " + x[3:])
    df.DEST_ZIP = df.DEST_ZIP.apply(lambda x: x if len(x) != 7 else x[:3] + " " + x[4:])
    df.ORIGIN_ZIP = df.ORIGIN_ZIP.apply(lambda x: x if len(x) != 7 else x[:3] + " " + x[4:])

    df = df.dropna(subset=["ORIGIN_ZIP", "DEST_ZIP"])

    yield df
