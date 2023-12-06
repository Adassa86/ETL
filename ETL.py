import glob
import pandas as pd
from datetime import datetime

#extract
#json extract function 


def extract_from_json(file_to_process):
    dataframe = pd.read_json(file_to_process)
    return dataframe

#extract function . Define the extract function that finds JSON file  and calls the function created above to extract data from them. Store the data in a pandas dataframe. Use the following list for the columns.
columns=['Name','Market Cap (US$ Billion)']
def extract():
    # Write your code here
    df = extract_from_json('bank_market_cap_1.json')
    #df.rename(columns={'Age': 'Years'}, inplace=True)
    return df 
df = extract()
df.head(5)


df2 = pd.read_csv('exchange_rates.csv')
df2.head(5)
print(df2.columns.tolist())

filtered_df = df2[df2['Unnamed: 0'] == 'GBP']
filtered_df
exchange_rate = filtered_df['Rates'].item()

#transform  Using exchange_rate and the exchange_rates.csv file find the exchange rate of USD to GBP. Write a transform function that
#Changes the Market Cap (US$ Billion) column from USD to GBP
#Rounds the Market Cap (US$ Billion)` column to 3 decimal places
#Rename Market Cap (US$ Billion) to Market Cap (GBP$ Billion)

def transform(df, exchange_rate):
    # Write your code here
    df['Market Cap (US$ Billion)'] = exchange_rate * df['Market Cap (US$ Billion)']
    df['Market Cap (US$ Billion)'] = round(df['Market Cap (US$ Billion)'],3)
    df.rename(columns={'Market Cap (US$ Billion)': 'Market Cap (GBP$ Billion)'}, inplace=True)
    return df

df = transform(df,exchange_rate)
df.head(5)

#Load Create a function that takes a dataframe and load it to a csv named bank_market_cap_gbp.csv. Make sure to set index to False.def load(df):
    # Write your code here
df.to_csv("bank_market_cap_gbp.csv", index = False)
load(df)
df3 = pd.read_csv('bank_market_cap_gbp.csv')
df3.head(5)

#Logging Function
#Write the logging function log to log your data:

def log(message):
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_entry = f"{timestamp}: {message}"

    with open("log.txt", "a") as log_file:
        log_file.write(log_entry + "\n")


#Running the ETL Process
#Log the process accordingly using the following "ETL Job Started" and "Extract phase Started"    

log("ETL Job Started")
log("Extract phase Started")

