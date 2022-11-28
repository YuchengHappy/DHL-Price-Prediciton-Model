import FE
import joblib


def pre_predict(df):
    df = FE.zip_to_LONG_LAT(df)
    df = FE.add_fuel(df)
    df = FE.add_country(df)
    df = FE.add_delta(df)
    df = FE.cross_country(df)
    df = FE.add_time_info(df)

    # TODO: import encoder from encoder folder
    month_encoder = joblib.load("xgb_model.sav")
    weekday_encoder = joblib.load("xgb_model.sav")
    hour_encoder = joblib.load("xgb_model.sav")
    minute_encoder = joblib.load("xgb_model.sav")
    one_hot_encoder = joblib.load("xgb_model.sav")

    df_pu_month = month_encoder.transform(df["PU_MONTH"])
    df_dl_month = month_encoder.transform(df["DL_MONTH"])
    df_pu_weekday = weekday_encoder.transform(df["PU_WEEKDAY"])
    df_dl_weekday = weekday_encoder.transform(df["DL_WEEKDAY"])
    df_pu_hour = hour_encoder.transform(df["PU_HOUR"])
    df_dl_hour = hour_encoder.transform(df["DL_HOUR"])
    df_pu_minute = minute_encoder.transform(df["PU_MINUTE"])
    df_dl_minute = minute_encoder.transform(df["DL_MINUTE"])
    df_one_hot = one_hot_encoder.transform(df["ACTUAL_MODE"])

    df = df.join(df_pu_month)
    df = df.join(df_dl_month)
    df = df.join(df_pu_weekday)
    df = df.join(df_dl_weekday)
    df = df.join(df_pu_hour)
    df = df.join(df_dl_hour)
    df = df.join(df_pu_minute)
    df = df.join(df_dl_minute)
    df = df.join(df_one_hot)

    df = df.drop(columns=["PU_MONTH", "DL_MONTH"])
    df = df.drop(columns=["PU_WEEKDAY", "DL_WEEKDAY"])
    df = df.drop(columns=["PU_HOUR", "DL_HOUR"])
    df = df.drop(columns=["PU_MINUTE", "DL_MINUTE"])
    df = df.drop(columns=["SHIPMENT_ID", "ACTUAL_MODE"])

    return df