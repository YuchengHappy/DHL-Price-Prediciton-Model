import os
import joblib
import numpy as np
import pre_predict
import pandas as pd
import xgboost as xgb
import streamlit as st
from predict_cost import predict


#### Main design starts here:
# Main title and instructions
st.title("Transportation Rate Prediction Tool")
st.write("You can choose the following methods to upload the inputs: ")
st.write("1. Click [>] to open the sidebar and upload the csv file")
st.write("2. Enter the values below manually")

# Divider
st.write("---")

# Sidebar
st.sidebar.header("Inputs Upload")
uploaded_file = st.sidebar.file_uploader("Upload your input csv file here", type = ["csv"])

if uploaded_file is not None:
    input_df = pd.read_csv(uploaded_file) # Need to fix to make sure the input data will have the correct format and order
else: 
    def inputs():
        
        # Subtitle "From" and "To"
        col1, col2 = st.columns(2)
        with col1:
            st.write("**From**")
        with col2:
            st.write("**To**")

        # "From" and "To" inputs: country and date
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            country_from = st.selectbox("Country", ("US", "CA"), key = "country_from")
        with col2:
            pick_date = st.date_input("Date", key = "pick_up") # need to restrict the pick_date cannot be later than deliv_date
        with col3:
            country_to = st.selectbox("Country", ("US", "CA"), key = "country_to")
        with col4:
            deliv_date = st.date_input("Date", key = "delivery")

        # "From" and "To" inputs: city and state
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            city_from = st.text_input("City", key = "city_from")
        with col2:
            state_from = st.selectbox("State",("AL", "AK", "AS", "AZ", "AR", "CA", "CO", "CT", "DE", "DC", "FM", "FL", "GA", "GU", "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MH", "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "MP", "OH", "OK", "OR", "PW", "PA", "PR", "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VI", "VA", "WA", "WV", "WI", "WY", "DC", "AS", "GU", "MP", "PR", "UM", "VI", "AB", "BC", "MB", "NB", "NL", "NT", "NS", "NU", "ON", "PE", "QC", "SK", "YT"), key = "state_from")
        with col3:
            city_to = st.text_input("City", key = "city_to")
        with col4:
            state_to = st.selectbox("State",("AL", "AK", "AS", "AZ", "AR", "CA", "CO", "CT", "DE", "DC", "FM", "FL", "GA", "GU", "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MH", "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "MP", "OH", "OK", "OR", "PW", "PA", "PR", "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VI", "VA", "WA", "WV", "WI", "WY", "DC", "AS", "GU", "MP", "PR", "UM", "VI", "AB", "BC", "MB", "NB", "NL", "NT", "NS", "NU", "ON", "PE", "QC", "SK", "YT"), key = "state_to")

        # "From" and "To" inputs: zip code
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            zip_from = st.text_input("Zip Code", value = "E.g. 99999 / ANA NAN", key = "zip_from")
        with col3:
            zip_to = st.text_input("Zip Code", value = "E.g. 99999 / ANA NAN", key = "zip_to")

        # Divider
        st.write("---")

        # Subtitle "Shipment"
        st.write("**Shipment**")

        # Shipment inputs 
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            case_input = st.number_input("Case")
        with col2:
            volume_input = st.number_input("Volume")
        with col3:
            weight_input = st.number_input("Weight")
        with col4:
            mode_input = st.selectbox("Actual Mode", ("TL", "LTL", "Intermodal"))

        data = {"Pick Up Date": pick_date, 
                "Origin City": city_from, 
                "Origin State": state_from,
                "Origin Country": country_from,
                "Origin Zip": zip_from,
                "Delivery Date": deliv_date,
                "Destination City": city_to, 
                "Destination State": state_to,
                "Destination Country": country_to, 
                "Destination Zip": zip_to, 
                "Case": case_input, 
                "Voulme": volume_input, 
                "Weight": weight_input, 
                "Mode": mode_input}

        features = pd.DataFrame(data, index = [0])
        return features
    input_df = inputs()
    input_df = pre_predict(input_df)

#### Data Transformation
# get src files (.py)





# Divider
st.write("---")

# Subtitle "Predicted Total Cost"
st.header("Predicted Total Cost")

# Print the iputs
st.write("Overview of Inputs")
st.write(input_df)


# Model and Prediction Functions go here... 





# Print the result
st.markdown(
    """ 
    <style>
    div.stButton > button:first-child {
    background-color: #13678a;color:white;font-size:15px;font-weight: 600;height:1.5em;width:15em;
    }
    </style>
    """, unsafe_allow_html=True
)

col1, col2, col3 = st.columns(3)
with col1:
    result = st.write("**Total Estimated Cost =**", "**$2,000**")
    # "$2,000" here will be replaced with the actual prediction result after we implement the model
with col3:
    if st.button("Get Estimated Cost"):
        cost = predict(input_df)
        st.text(cost)
    # when "Get Estimated Cost" is clicked, make the prediction and store it 
    # if st.button("Get Estimated Cost"): 
        # result = prediction(Gender, Married, ApplicantIncome, LoanAmount, Credit_History) 
        # st.success('Your loan is {}'.format(result))
        # print(LoanAmount)



