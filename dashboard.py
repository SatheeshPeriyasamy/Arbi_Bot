import streamlit as st
from pymongo import MongoClient
import pandas as pd


mongo_client = MongoClient('mongodb://localhost:27017/')
db = mongo_client['arbitrage_bot']
trades_collection = db['trades']

def load_data():
    trades = list(trades_collection.find({}, {'_id': 0}))
    return pd.DataFrame(trades)

def app():
    st.title('Arbitrage Bot Dashboard')
    trades_data = load_data()
    if not trades_data.empty:
        st.write(trades_data)
        st.line_chart(trades_data[['spot_price', 'futures_price']])
    else:
        st.write("No trades executed yet.")

if __name__ == "__main__":
    app()
