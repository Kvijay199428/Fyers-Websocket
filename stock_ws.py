import configparser
import psycopg2
from psycopg2 import Error
from fyers_apiv3.FyersWebsocket import data_ws
from datetime import datetime
import os
import json
import requests

class SymbolManager:
    def __init__(self):
        self.symbol_dir = "api/symbol"
        os.makedirs(self.symbol_dir, exist_ok=True)
        
    def download_symbol_file(self):
        """Download the symbol master file"""
        url = "https://public.fyers.in/sym_details/NSE_CM_sym_master.json"
        symbol_path = os.path.join(self.symbol_dir, "NSE_CM_sym_master.json")
        
        try:
            print("Downloading symbol master file...")
            response = requests.get(url)
            response.raise_for_status()
            
            content = response.text
            content = content.strip()
            if content.endswith(','):
                content = content[:-1]
            
            if not content.startswith('{'):
                content = '{' + content
            if not content.endswith('}'):
                content = content + '}'
            
            with open(symbol_path, 'w', encoding='utf-8') as f:
                f.write(content)
            
            print("Symbol master file downloaded and cleaned successfully")
            return symbol_path
            
        except Exception as e:
            raise Exception(f"Error downloading symbol file: {str(e)}")

    def read_symbol_list(self):
        """Read and parse the local symbol master file"""
        try:
            local_file = self.download_symbol_file()
            
            with open(local_file, 'r') as f:
                data = json.load(f)
            
            equity_symbols = [symbol for symbol in data.keys() if symbol.endswith('-EQ')]
            
            print(f"Found {len(equity_symbols)} equity symbols")
            if equity_symbols:
                print("Sample symbols:", equity_symbols[:5])
            
            return equity_symbols
            
        except Exception as e:
            raise Exception(f"Error reading symbol list: {str(e)}")

class DatabaseManager:
    def __init__(self, ini_path):
        self.config = self._read_config(ini_path)
        self.connection = None
        self.setup_database()
        self.data_cache = {}

    def _read_config(self, ini_path):
        config = configparser.ConfigParser()
        config.read(ini_path)
        return config['postgresql']

    def setup_database(self):
        try:
            self.connection = psycopg2.connect(
                host=self.config['host'],
                port=self.config['port'],
                user=self.config['user'],
                password=self.config['password'],
                database=self.config['database']
            )
        except psycopg2.OperationalError:
            temp_conn = psycopg2.connect(
                host=self.config['host'],
                port=self.config['port'],
                user=self.config['user'],
                password=self.config['password'],
                database='postgres'
            )
            temp_conn.autocommit = True
            with temp_conn.cursor() as cursor:
                cursor.execute(f"CREATE DATABASE {self.config['database']}")
            temp_conn.close()
            
            self.connection = psycopg2.connect(
                host=self.config['host'],
                port=self.config['port'],
                user=self.config['user'],
                password=self.config['password'],
                database=self.config['database']
            )
        
        self.connection.autocommit = True

    def create_table(self, symbol):
        # Sanitize the table name: replace special characters with underscores
        table_name = (
            symbol
            .replace(':', '_')
            .replace('-', '_')
            .replace('&', '_')
            .replace(' ', '_')
            # Add any other special characters that need to be replaced
        )
        
        # Ensure table name starts with a letter or underscore
        if not table_name[0].isalpha() and table_name[0] != '_':
            table_name = 'symbol_' + table_name
        
        with self.connection.cursor() as cursor:
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id SERIAL PRIMARY KEY,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    
                    -- Market data fields
                    ltp FLOAT,
                    vol_traded_today BIGINT,
                    last_traded_time BIGINT,
                    exch_feed_time BIGINT,
                    bid_size INTEGER,
                    ask_size INTEGER,
                    bid_price FLOAT,
                    ask_price FLOAT,
                    last_traded_qty INTEGER,
                    tot_buy_qty BIGINT,
                    tot_sell_qty BIGINT,
                    avg_trade_price FLOAT,
                    low_price FLOAT,
                    high_price FLOAT,
                    lower_ckt FLOAT,
                    upper_ckt FLOAT,
                    open_price FLOAT,
                    prev_close_price FLOAT,
                    ch FLOAT,
                    chp FLOAT,
                    
                    -- Depth fields
                    bid_price1 FLOAT, bid_price2 FLOAT, bid_price3 FLOAT, bid_price4 FLOAT, bid_price5 FLOAT,
                    ask_price1 FLOAT, ask_price2 FLOAT, ask_price3 FLOAT, ask_price4 FLOAT, ask_price5 FLOAT,
                    bid_size1 INTEGER, bid_size2 INTEGER, bid_size3 INTEGER, bid_size4 INTEGER, bid_size5 INTEGER,
                    ask_size1 INTEGER, ask_size2 INTEGER, ask_size3 INTEGER, ask_size4 INTEGER, ask_size5 INTEGER,
                    bid_order1 INTEGER, bid_order2 INTEGER, bid_order3 INTEGER, bid_order4 INTEGER, bid_order5 INTEGER,
                    ask_order1 INTEGER, ask_order2 INTEGER, ask_order3 INTEGER, ask_order4 INTEGER, ask_order5 INTEGER,
                    
                    type VARCHAR(10)
                )
            """)

    def update_cache_and_insert(self, data, symbol, update_type):
        if symbol not in self.data_cache:
            self.data_cache[symbol] = {'symbol': symbol}
            
        self.data_cache[symbol].update(data)
        
        if self.has_required_fields(self.data_cache[symbol]):
            self.insert_combined_data(self.data_cache[symbol], symbol)
            self.data_cache[symbol] = {'symbol': symbol}

    def has_required_fields(self, data):
        market_fields = ['ltp', 'vol_traded_today', 'last_traded_time']
        depth_fields = ['bid_price1', 'ask_price1', 'bid_size1']
        
        has_market = any(field in data for field in market_fields)
        has_depth = any(field in data for field in depth_fields)
        
        return has_market and has_depth

    def insert_combined_data(self, data, symbol):
        table_name = symbol.replace(':', '_').replace('-', '_')
        
        with self.connection.cursor() as cursor:
            cursor.execute(f"""
                INSERT INTO {table_name} (
                    ltp, vol_traded_today, last_traded_time, exch_feed_time,
                    bid_size, ask_size, bid_price, ask_price, last_traded_qty,
                    tot_buy_qty, tot_sell_qty, avg_trade_price, low_price,
                    high_price, lower_ckt, upper_ckt, open_price, prev_close_price,
                    ch, chp, bid_price1, bid_price2, bid_price3, bid_price4, bid_price5,
                    ask_price1, ask_price2, ask_price3, ask_price4, ask_price5,
                    bid_size1, bid_size2, bid_size3, bid_size4, bid_size5,
                    ask_size1, ask_size2, ask_size3, ask_size4, ask_size5,
                    bid_order1, bid_order2, bid_order3, bid_order4, bid_order5,
                    ask_order1, ask_order2, ask_order3, ask_order4, ask_order5,
                    type
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s
                )
            """, (
                data.get('ltp'), data.get('vol_traded_today'),
                data.get('last_traded_time'), data.get('exch_feed_time'),
                data.get('bid_size'), data.get('ask_size'),
                data.get('bid_price'), data.get('ask_price'),
                data.get('last_traded_qty'), data.get('tot_buy_qty'),
                data.get('tot_sell_qty'), data.get('avg_trade_price'),
                data.get('low_price'), data.get('high_price'),
                data.get('lower_ckt'), data.get('upper_ckt'),
                data.get('open_price'), data.get('prev_close_price'),
                data.get('ch'), data.get('chp'),
                data.get('bid_price1'), data.get('bid_price2'),
                data.get('bid_price3'), data.get('bid_price4'),
                data.get('bid_price5'), data.get('ask_price1'),
                data.get('ask_price2'), data.get('ask_price3'),
                data.get('ask_price4'), data.get('ask_price5'),
                data.get('bid_size1'), data.get('bid_size2'),
                data.get('bid_size3'), data.get('bid_size4'),
                data.get('bid_size5'), data.get('ask_size1'),
                data.get('ask_size2'), data.get('ask_size3'),
                data.get('ask_size4'), data.get('ask_size5'),
                data.get('bid_order1'), data.get('bid_order2'),
                data.get('bid_order3'), data.get('bid_order4'),
                data.get('bid_order5'), data.get('ask_order1'),
                data.get('ask_order2'), data.get('ask_order3'),
                data.get('ask_order4'), data.get('ask_order5'),
                data.get('type')
            ))

    def close(self):
        if self.connection:
            self.connection.close()

def read_access_token():
    """Read access token from file"""
    token_path = "api/token/access_token"
    try:
        with open(token_path, 'r') as f:
            return f.read().strip()
    except Exception as e:
        raise Exception(f"Error reading access token: {str(e)}")

def main():
    # Create necessary directories
    os.makedirs("api/token", exist_ok=True)
    os.makedirs("api/ini", exist_ok=True)
    
    # Create config file if it doesn't exist
    ini_content = """[postgresql]
    host = localhost
    port = 5432
    user = postgres
    password = admin
    database = stock
    """
    with open('api/ini/stock.ini', 'w') as f:
        f.write(ini_content)

    # Initialize managers
    symbol_manager = SymbolManager()
    db_manager = DatabaseManager('api/ini/stock.ini')

    # Get symbols and access token
    symbols = symbol_manager.read_symbol_list()
    access_token = read_access_token()

    # Create tables for each symbol
    for symbol in symbols:
        symbol_with_prefix = f"{symbol}"
        db_manager.create_table(symbol_with_prefix)

    def onmessage(message):
        """Handle incoming messages from the WebSocket."""
        print("Response:", message)
        
        if message.get('type') in ['cn', 'ful', 'sub']:
            return
            
        symbol = message.get('symbol')
        if not symbol:
            return
            
        if message.get('type') == 'sf':
            db_manager.update_cache_and_insert(message, symbol, 'market')
        elif message.get('type') == 'dp':
            db_manager.update_cache_and_insert(message, symbol, 'depth')

    def onopen():
        """Subscribe to data types and symbols upon WebSocket connection."""
        for data_type in ["SymbolUpdate", "DepthUpdate"]:
            fyers.subscribe(symbols=[f"{sym}" for sym in symbols], data_type=data_type)
        fyers.keep_running()

    def onerror(message):
        """Handle WebSocket errors."""
        print("Error:", message)

    def onclose(message):
        """Handle WebSocket connection close."""
        print("Connection closed:", message)
        db_manager.close()

    # Initialize FyersDataSocket
    fyers = data_ws.FyersDataSocket(
        access_token=access_token,
        log_path="",
        litemode=False,
        write_to_file=False,
        reconnect=True,
        on_connect=onopen,
        on_close=onclose,
        on_error=onerror,
        on_message=onmessage
    )

    # Connect to the WebSocket
    fyers.connect()

if __name__ == "__main__":
    main()