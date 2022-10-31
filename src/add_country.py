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