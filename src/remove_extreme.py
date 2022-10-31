def remove_extreme(df):

    # Exclude Extreme Value in cases
    q_cases_99 = df["CASES"].quantile(0.99)
    EXTREME = df["CASES"] > q_cases_99

    # Exclude Extreme Value in distance
    q_distance_99 = df["DISTANCE"].quantile(0.99)
    q_distance_01 = df["DISTANCE"].quantile(0.01)
    EXTREME = EXTREME + (df["DISTANCE"] > q_distance_99)
    EXTREME = EXTREME + (df["DISTANCE"] < q_distance_01)

    # Exclude Extreme Value in weight
    q_weight_95 = df["WEIGHT"].quantile(0.99)
    q_weight_05 = df["WEIGHT"].quantile(0.01)
    EXTREME = EXTREME + (df["WEIGHT"] > q_weight_95)
    EXTREME = EXTREME + (df["WEIGHT"] < q_weight_05)

    # Exclude Extreme Value in volume
    q_volume_95 = df["VOLUME"].quantile(0.99)
    q_volume_05 = df["VOLUME"].quantile(0.01)
    EXTREME = EXTREME + (df["VOLUME"] > q_weight_95)
    EXTREME = EXTREME + (df["VOLUME"] < q_weight_05)

    # Exclude Extreme Value in LINEHAUL_COSTS
    q_linehaul_95 = df["LINEHAUL_COSTS"].quantile(0.99)
    q_linehaul_05 = df["LINEHAUL_COSTS"].quantile(0.01)
    EXTREME = EXTREME + (df["LINEHAUL_COSTS"] > q_linehaul_95)
    EXTREME = EXTREME + (df["LINEHAUL_COSTS"] < q_linehaul_05)

    # Exclude Extreme Value in FUEL_COSTS
    q_linehaul_95 = df["FUEL_COSTS"].quantile(0.99)
    EXTREME = EXTREME + (df["FUEL_COSTS"] > q_linehaul_95)

    # Exclude Extreme Value in ACC._COSTS
    q_acc_95 = df["ACC._COSTS"].quantile(0.99)
    EXTREME = EXTREME + (df["ACC._COSTS"] > q_acc_95)

    # Exclude Extreme Value in "DURATION_MINUTE"
    q_acc_99 = df["DURATION_MINUTE"].quantile(0.99)
    EXTREME = EXTREME + (df["DURATION_MINUTE"] > q_acc_99)

    df = df[~EXTREME]

    yield df
