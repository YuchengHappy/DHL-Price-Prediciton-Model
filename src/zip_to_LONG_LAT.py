import pgeocode
import numpy as np


def zip_to_LONG_LAT(df):

    us_nomi = pgeocode.Nominatim("us")
    ca_nomi = pgeocode.Nominatim("ca")

    def us_origin_zip_decode(zip):
        return {zip: {"ORIGIN_LAT": us_nomi.query_postal_code(zip)["latitude"],
                "ORIGIN_LONG": us_nomi.query_postal_code(zip)["longitude"]}}

    def us_dest_zip_decode(zip):
        return {zip: {"DEST_LAT": us_nomi.query_postal_code(zip)["latitude"],
                "DEST_LONG": us_nomi.query_postal_code(zip)["longitude"]}}

    def ca_origin_zip_decode(zip):
        return {zip: {"ORIGIN_LAT": ca_nomi.query_postal_code(zip)["latitude"],
                "ORIGIN_LONG": ca_nomi.query_postal_code(zip)["longitude"]}}

    def ca_dest_zip_decode(zip):
        return {zip: {"DEST_LAT": ca_nomi.query_postal_code(zip)["latitude"],
                "DEST_LONG": ca_nomi.query_postal_code(zip)["longitude"]}}

    ca_origin = df[df["ORIGIN_COUNTRY"] == "ca"]
    ca_origin = ca_origin["ORIGIN_ZIP"].unique()

    ca_dest = df[df["DEST_COUNTRY"] == "ca"]
    ca_dest = ca_dest["DEST_ZIP"].unique()

    origin_info = {}
    for i in ca_origin:
        origin_info.update(ca_origin_zip_decode(i))

    dest_info = {}
    for i in ca_dest:
        dest_info.update(ca_dest_zip_decode(i))

    us_origin = df[df["ORIGIN_COUNTRY"] == "us"]
    us_origin = us_origin["ORIGIN_ZIP"].unique()

    us_dest = df[df["DEST_COUNTRY"] == "us"]
    us_dest = us_dest["DEST_ZIP"].unique()

    for i in us_origin:
        origin_info.update(us_origin_zip_decode(i))

    for i in us_dest:
        dest_info.update(us_dest_zip_decode(i))

    origin_key = list(origin_info.keys())
    dest_key = list(dest_info.keys())

    def assign_dest_lat(zipcode):
        if zipcode in dest_key:
            return dest_info.get(zipcode).get("DEST_LAT")
        else:
            return np.nan

    def assign_dest_long(zipcode):
        if zipcode in dest_key:
            return dest_info.get(zipcode).get("DEST_LONG")
        else:
            return np.nan

    def assign_origin_lat(zipcode):
        if zipcode in origin_key:
            return origin_info.get(zipcode).get("ORIGIN_LAT")
        else:
            return np.nan

    def assign_origin_long(zipcode):
        if zipcode in origin_key:
            return origin_info.get(zipcode).get("ORIGIN_LONG")
        else:
            return np.nan

    df.ORIGIN_LAT = df.ORIGIN_ZIP.apply(lambda x: assign_origin_lat(x))
    df.ORIGIN_LONG = df.ORIGIN_ZIP.apply(lambda x: assign_origin_long(x))

    df.DEST_LAT = df.DEST_ZIP.apply(lambda x: assign_dest_lat(x))
    df.DEST_LONG = df.DEST_ZIP.apply(lambda x: assign_dest_long(x))

    df = df.dropna(subset=["ORIGIN_LAT", "DEST_LAT"])

    yield df
