import pandas_datareader.data as pdr
from sqlalchemy import create_engine
import mysql.connector
import yfinance as yf
import pandas as pd
import dotenv
import os

yf.pdr_override()

index_tickers = ['^STOXX50E', '^N225', 'DXY']
macro_tickers = ['SP500', 'NASDAQCOM', 'DJIA', 'GDP', 'CPIAUCSL', 'DFEDTARU', 'PPICOR', 'VIXCLS', 'PAYEMS', 'UNRATE', 'WTISPLC', 'JTSJOL', 'UMCSENT', 'DEXSZUS', 'DEXJPUS', 'DEXUSEU', 'DEXUSUK', 'DEXCHUS', 'USSTHPI']

index_data = pdr.get_data_yahoo(index_tickers, start='2013-1-1', end='2022-12-31')
macro_data = pdr.get_data_fred(macro_tickers, start='2013-1-1', end='2022-12-31')

df = pd.concat([index_data, macro_data], axis=1, ignore_index=True)

dotenv.load_dotenv(r'C:/vault/.my_w')
user = 'root'
pw = os.getenv("mysqlpw")
database = 'economic_data'

conn = mysql.connector.connect(host='127.0.0.1', user='root', password=pw)

cursor = conn.cursor()

engine = create_engine(f'mysql+mysqlconnector://{user}:{pw}@localhost/{database}')

df.to_sql(name=database, con=engine, if_exists="replace")
