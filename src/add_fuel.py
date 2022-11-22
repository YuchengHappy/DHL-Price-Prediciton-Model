import requests
import pandas as pd

def add_fuel(df):
	#Setup

	#Reading the dataframe
	url = 'https://fred.stlouisfed.org/graph/fredgraph.xls?id=CHXRSA'
	r = requests.get(url)
	open('temp.xls', 'wb').write(r.content)
	fuel_df = pd.read_excel('temp.xls', sheet_name="FRED Graph", skiprows = 9)

	fuel_df["Frequency: Monthly"] =  pd.to_datetime(fuel_df["Frequency: Monthly"], errors='coerce')
	fuel_df = fuel_df.dropna()
	fuel_df["PU_MONTH"] = fuel_df["Frequency: Monthly"].dt.month 
	fuel_df["PU_YEAR"] = fuel_df["Frequency: Monthly"].dt.year

	fuel_df = fuel_df.rename(columns={'Unnamed: 1': 'FUEL_PRICE'})
	fuel_df = fuel_df.drop(columns=["Frequency: Monthly"])
	df = pd.merge(df, fuel_df, on=["PU_MONTH", "PU_YEAR"])

	return df