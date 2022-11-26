# Import packages
import joblib
import numpy as np
import pandas as pd
import xgboost as xgb
from sklearn.metrics import mean_squared_error
from sklearn.model_selection import train_test_split

# Upload 1.4million row dataset and remove non-numeric variables 
data = pd.read_csv("FE_Done_Final_V1_NOV20.csv")
data.drop(['Unnamed: 0', 'FUEL_COSTS', 'ACC._COSTS', 'LINEHAUL_COSTS'], axis=1, inplace=True)

# Reorder columns 
order = [4,0,1,2,3,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36]
data = data[[data.columns[i] for i in order]]

#Split data into dependent and independent variable sets
X, y = data.iloc[:,1:37],data.iloc[:,0]

#Transform data into matrix for efficient xgboost training 
data_dmatrix = xgb.DMatrix(data=X,label=y)

#Split data into test and training set 
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=123)

#Used best features to train model, but increased the n_estimators to 1000
xgb_cv = xgb.XGBRegressor(subsample = 0.8999999999999999, n_estimators = 1000, max_depth = 20, learning_rate = 0.01, colsample_bytree = 0.7, colsample_bylevel = 0.8999999999999999, booster = 'gbtree')

#Fit model with best hyperparameters, but increase n_estimators to 1000
xgb_cv.fit(X_train,y_train)
preds = xgb_cv.predict(X_test)

# Save Model
joblib.dump(xgb_cv, "xgb_model.sav") 