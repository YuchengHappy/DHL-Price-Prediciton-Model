import joblib
 
def predict(input_df):
    xgb_cv = joblib.load("xgb_model.sav")
    return xgb_cv.predict(input_df) 