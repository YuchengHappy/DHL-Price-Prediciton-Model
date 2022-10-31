import numpy as np
import pandas as pd


# input type: pandas dataframe
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

    # output type: pandas dataframe
    yield df
