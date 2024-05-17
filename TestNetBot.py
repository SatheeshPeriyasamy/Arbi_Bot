import asyncio
import logging
from datetime import datetime
from pymongo import MongoClient, errors as pymongo_errors
from binance import AsyncClient, BinanceSocketManager
from binance.exceptions import BinanceAPIException
import aiohttp
import requests


logging.basicConfig(filename='arbitrage_bot.log', level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')


try:
    mongo_client = MongoClient('mongodb://localhost:27017/')
    db = mongo_client['arbitrage_bot']
    trades_collection = db['trades']
    logging.info("Connected to MongoDB")
except pymongo_errors.ConnectionFailure as e:
    logging.error(f"Could not connect to MongoDB: {e}")
    exit(1)

BINANCE_FUTURES_TESTNET_API_URL = 'https://testnet.binancefuture.com/fapi/v1'

price_dict = {
    'btcusdt': {'spot': None, 'futures': None},
    'ethusdt': {'spot': None, 'futures': None},
    'xrpusdt': {'spot': None, 'futures': None},
    'dogeusdt': {'spot': None, 'futures': None},
    'adausdt': {'spot': None, 'futures': None},
    'shibusdt': {'spot': None, 'futures': None},
    'avaxusdt': {'spot': None, 'futures': None},
    'dotusdt': {'spot': None, 'futures': None},
    'linkusdt': {'spot': None, 'futures': None},
    'bchusdt': {'spot': None, 'futures': None},
    'ltcusdt': {'spot': None, 'futures': None}
}

quantity_precision = {}
lot_size = {}
step_size = {}
max_qty = {}

async def fetch_lot_and_step_sizes():
    url = "https://testnet.binance.vision/api/v3/exchangeInfo"
    response = requests.get(url)
    exchange_info = response.json()

    for symbol_info in exchange_info['symbols']:
        symbol = symbol_info['symbol'].lower()
        for filter in symbol_info['filters']:
            if filter['filterType'] == 'LOT_SIZE':
                lot_size[symbol] = float(filter['minQty'])
                step_size[symbol] = float(filter['stepSize'])
                max_qty[symbol] = float(filter['maxQty'])
                quantity_precision[symbol] = max(len(filter['stepSize'].rstrip('0').split('.')[-1]), 0)

async def fetch_futures_prices(symbol):
    async with aiohttp.ClientSession() as session:
        async with session.get(f"{BINANCE_FUTURES_TESTNET_API_URL}/ticker/price?symbol={symbol}") as response:
            data = await response.json()
            if 'price' in data:
                return float(data['price'])
            else:
                logging.error(f"Error fetching futures price for {symbol}: 'price' key not found in response")
                return None

def adjust_quantity_for_lot_size(symbol, quantity):
    #Mentioned in USDT
    min_qty = lot_size.get(symbol, 0.01)  
    step = step_size.get(symbol, 0.01)  
    max_q = max_qty.get(symbol, 1000000)  
    if quantity < min_qty:
        quantity = min_qty
    if quantity > max_q:
        quantity = max_q
    increment = int(quantity / step) * step
    if increment < quantity:
        increment += step
    return round(increment, quantity_precision.get(symbol, 8))

async def execute_arbitrage(client, symbol, spot_price, futures_price):
    min_notional_value = 10  
    quantity = 0.01  
    notional_value = spot_price * quantity

    if notional_value < min_notional_value:
        quantity = min_notional_value / spot_price

    quantity = adjust_quantity_for_lot_size(symbol, quantity)

    try:
        if spot_price < futures_price:
            buy_order = await client.create_order(
                symbol=symbol,
                side='BUY',
                type='MARKET',
                quantity=quantity)

            sell_order = await client.futures_create_order(
                symbol=symbol + "_PERP",
                side='SELL',
                type='MARKET',
                quantity=quantity)

            logging.info(f"Arbitrage executed: Buy {symbol} on spot at {spot_price}, Sell on futures at {futures_price}")
            try:
                trades_collection.insert_one({
                    "type": "arbitrage",
                    "symbol": symbol,
                    "spot_order": buy_order,
                    "futures_order": sell_order,
                    "spot_price": spot_price,
                    "futures_price": futures_price,
                    "timestamp": datetime.utcnow()
                })
                logging.info(f"Trade inserted into MongoDB: {symbol}")
            except pymongo_errors.PyMongoError as e:
                logging.error(f"Error inserting trade into MongoDB: {e}")

        elif spot_price > futures_price:
            sell_order = await client.create_order(
                symbol=symbol,
                side='SELL',
                type='MARKET',
                quantity=quantity)

            buy_order = await client.futures_create_order(
                symbol=symbol + "_PERP",
                side='BUY',
                type='MARKET',
                quantity=quantity)

            logging.info(f"Reverse Arbitrage executed: Sell {symbol} on spot at {spot_price}, Buy on futures at {futures_price}")
            try:
                trades_collection.insert_one({
                    "type": "reverse_arbitrage",
                    "symbol": symbol,
                    "spot_order": sell_order,
                    "futures_order": buy_order,
                    "spot_price": spot_price,
                    "futures_price": futures_price,
                    "timestamp": datetime.utcnow()
                })
                logging.info(f"Trade inserted into MongoDB: {symbol}")
            except pymongo_errors.PyMongoError as e:
                logging.error(f"Error inserting trade into MongoDB: {e}")

    except BinanceAPIException as e:
        logging.error(f"Error executing trade: {e}")
    except Exception as e:
        logging.error(f"Error executing trade: {e}")

async def check_and_execute_arbitrage(client, symbol):
    if price_dict[symbol]['spot'] is not None and price_dict[symbol]['futures'] is not None:
        logging.debug(f"Executing arbitrage for {symbol.upper()}: spot price {price_dict[symbol]['spot']}, futures price {price_dict[symbol]['futures']}")
        await execute_arbitrage(client, symbol.upper(), price_dict[symbol]['spot'], price_dict[symbol]['futures'])
        price_dict[symbol] = {'spot': None, 'futures': None}  

async def main():
    client = await AsyncClient.create('gajrEZysWwZDWgSVsGetUBBdmDkwJJCOPUtJPt9HsNg8JK8iHja1TfCQVu5danI5', 'wBKgUPnCYgKq0b5EkpYfnwXw6JsrLBJn9eKZLQuPr7t2fN6xd8zNxWK1tqPlCnaS', testnet=True)

    await fetch_lot_and_step_sizes()

    bm = BinanceSocketManager(client)
    
    pairs_to_monitor = [
        'btcusdt', 'ethusdt', 'xrpusdt', 'dogeusdt', 'adausdt', 
        'shibusdt', 'avaxusdt', 'dotusdt', 'linkusdt', 'bchusdt', 'ltcusdt'
    ]
    
    spot_streams = [f"{pair}@ticker" for pair in pairs_to_monitor]
    
    async def handle_spot_message(msg):
        data = msg['data']
        symbol = data['s'].lower()
        price = float(data['c'])
        price_dict[symbol]['spot'] = price
        logging.debug(f"Spot price updated for {symbol}: {price}")
        await check_and_execute_arbitrage(client, symbol)

    async def update_futures_prices():
        while True:
            for pair in pairs_to_monitor:
                futures_symbol = pair.replace('usdt', 'USDT')
                futures_price = await fetch_futures_prices(futures_symbol)
                if futures_price:
                    price_dict[pair]['futures'] = futures_price
                    logging.debug(f"Futures price updated for {pair}: {futures_price}")
            await asyncio.sleep(1)  

    spot_task = None
    futures_task = None

    try:
        async with bm.multiplex_socket(spot_streams) as spot_stream:
            spot_task = asyncio.create_task(handle_stream(spot_stream, handle_spot_message))
            futures_task = asyncio.create_task(update_futures_prices())
            await asyncio.gather(spot_task, futures_task)
    finally:
        if spot_task:
            spot_task.cancel()
        if futures_task:
            futures_task.cancel()
        await client.close_connection()

async def handle_stream(stream, handler):
    while True:
        msg = await stream.recv()
        await handler(msg)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        logging.error(f"Exception in main: {e}")
        asyncio.run(main())
