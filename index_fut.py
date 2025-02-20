import configparser
import psycopg2
from psycopg2 import Error
from fyers_apiv3.FyersWebsocket import data_ws
from datetime import datetime
import os
import json
import requests
import logging
from logging.handlers import RotatingFileHandler

# Setup logging configuration
def setup_logging():
    """Configure logging with separate files for different symbol types"""
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)
    
    # Configure main logger
    logging.basicConfig(level=logging.INFO)
    
    # Create formatters
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    # Setup handlers with a lock to prevent file access conflicts
    handlers = {}
    for log_name in ['symbol_responses', 'index_symbols', 'fut_symbols']:
        handler = RotatingFileHandler(
            os.path.join(log_dir, f'{log_name}.log'),
            maxBytes=10*1024*1024,  # 10MB
            backupCount=5,
            delay=True  # Delay file creation until first log
        )
        handler.setFormatter(formatter)
        logger = logging.getLogger(log_name)
        logger.propagate = False  # Prevent duplicate logging
        logger.addHandler(handler)
        handlers[log_name] = logger
    
    return handlers['symbol_responses'], handlers['index_symbols'], handlers['fut_symbols']

class SymbolManager:
    def __init__(self):
        self.symbol_dir = "api/symbol"
        os.makedirs(self.symbol_dir, exist_ok=True)
        
    def download_symbol_files(self):
        """Download both index and futures symbol master files"""
        files_to_download = {
            'index': {
                'url': "https://public.fyers.in/sym_details/NSE_CM_sym_master.json",
                'path': os.path.join(self.symbol_dir, "NSE_CM_sym_master.json")
            },
            'futures': {
                'url': "https://public.fyers.in/sym_details/NSE_FO_sym_master.json",
                'path': os.path.join(self.symbol_dir, "NSE_FO_sym_master.json")
            }
        }
        
        downloaded_files = {}
        
        for market_type, file_info in files_to_download.items():
            try:
                print(f"Downloading {market_type} symbol master file...")
                response = requests.get(file_info['url'])
                response.raise_for_status()
                
                content = response.text
                content = content.strip()
                if content.endswith(','):
                    content = content[:-1]
                
                if not content.startswith('{'):
                    content = '{' + content
                if not content.endswith('}'):
                    content = content + '}'
                
                with open(file_info['path'], 'w', encoding='utf-8') as f:
                    f.write(content)
                
                print(f"{market_type.capitalize()} symbol master file downloaded and cleaned successfully")
                downloaded_files[market_type] = file_info['path']
                
            except Exception as e:
                print(f"Error downloading {market_type} symbol file: {str(e)}")
                
        return downloaded_files

    def read_symbol_list(self):
        """Read and parse both local symbol master files"""
        try:
            downloaded_files = self.download_symbol_files()
            all_symbols = []
            
            # Read index symbols
            if 'index' in downloaded_files:
                with open(downloaded_files['index'], 'r') as f:
                    index_data = json.load(f)
                index_symbols = [symbol for symbol in index_data.keys() if symbol.endswith('-INDEX')]
                all_symbols.extend(index_symbols)
                print(f"Found {len(index_symbols)} index symbols")
                if index_symbols:
                    print("Sample index symbols:", index_symbols[:5])
            
            # Read futures symbols
            if 'futures' in downloaded_files:
                with open(downloaded_files['futures'], 'r') as f:
                    futures_data = json.load(f)
                futures_symbols = [symbol for symbol in futures_data.keys() if symbol.endswith('FUT')]
                all_symbols.extend(futures_symbols)
                print(f"Found {len(futures_symbols)} futures symbols")
                if futures_symbols:
                    print("Sample futures symbols:", futures_symbols[:5])
            
            print(f"Total symbols: {len(all_symbols)}")
            return all_symbols
            
        except Exception as e:
            raise Exception(f"Error reading symbol list: {str(e)}")

class DatabaseManager:
    def __init__(self, ini_path):
        self.config = self._read_config(ini_path)
        self.connection = None
        self.setup_database()
        self.data_cache = {}
        self.symbol_logger, self.index_logger, self.fut_logger = setup_logging()

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
        table_name = (
            symbol
            .replace(':', '_')
            .replace('-', '_')
            .replace('&', '_')
            .replace(' ', '_')
        )
        
        if not table_name[0].isalpha() and table_name[0] != '_':
            table_name = 'symbol_' + table_name
        
        with self.connection.cursor() as cursor:
            if symbol.endswith('-INDEX'):
                # Create table for INDEX symbols with limited fields
                cursor.execute(f"""
                    CREATE TABLE IF NOT EXISTS {table_name} (
                        id SERIAL PRIMARY KEY,
                        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        ltp FLOAT,
                        prev_close_price FLOAT,
                        ch FLOAT,
                        chp FLOAT,
                        exch_feed_time BIGINT,
                        high_price FLOAT,
                        low_price FLOAT,
                        open_price FLOAT,
                        type VARCHAR(10)
                    )
                """)
            else:
                # Create table for FUT symbols with all fields
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
        try:
            self.symbol_logger.info(f"Received data for {symbol}: {data}")
            
            if symbol not in self.data_cache:
                self.data_cache[symbol] = {'symbol': symbol}
            
            # Update cache with new data
            self.data_cache[symbol].update(data)
            
            if symbol.endswith('-INDEX'):
                self.index_logger.info(f"Processing INDEX symbol: {symbol}")
                if self.has_required_fields_index(self.data_cache[symbol]):
                    try:
                        self.insert_index_data(self.data_cache[symbol], symbol)
                        self.index_logger.info(f"Successfully inserted INDEX data for {symbol}")
                        self.data_cache[symbol] = {'symbol': symbol}
                    except Exception as e:
                        self.index_logger.error(f"Database insertion error for INDEX {symbol}: {str(e)}")
            else:
                self.fut_logger.info(f"Processing FUT symbol: {symbol}")
                
                # For market data updates
                if update_type == 'market':
                    current_data = self.data_cache[symbol]
                    if self.has_required_fields_fut(current_data):
                        try:
                            self.insert_fut_data(current_data, symbol)
                            self.fut_logger.info(f"Successfully inserted FUT market data for {symbol}")
                            self.data_cache[symbol] = {'symbol': symbol}
                        except Exception as e:
                            self.fut_logger.error(f"Database insertion error for FUT {symbol}: {str(e)}")
                    else:
                        self.fut_logger.debug(f"Current cache state for {symbol}: {current_data}")
                
        except Exception as e:
            self.symbol_logger.error(f"Error in update_cache_and_insert: {str(e)}", exc_info=True)

    def _check_missing_fields_index(self, data):
        """Helper method to check which required fields are missing for INDEX symbols"""
        required_fields = ['ltp', 'prev_close_price', 'ch', 'chp']
        return {field: field in data for field in required_fields}

    def _check_missing_fields_fut(self, data):
        """Helper method to check which required fields are missing for FUT symbols"""
        market_fields = ['ltp', 'vol_traded_today', 'last_traded_time']
        depth_fields = ['bid_price1', 'ask_price1', 'bid_size1']
        return {
            'market_fields': {field: field in data for field in market_fields},
            'depth_fields': {field: field in data for field in depth_fields}
        }

    def has_required_fields_index(self, data):
        """Check for required fields for INDEX symbols"""
        required_fields = ['ltp', 'prev_close_price', 'ch', 'chp']
        return all(field in data for field in required_fields)

    def has_required_fields_fut(self, data):
        """Check for required fields for FUT symbols with more detailed logging"""
        market_fields = [
            'ltp', 'vol_traded_today', 'last_traded_time', 'bid_price', 'ask_price',
            'bid_size', 'ask_size', 'last_traded_qty'
        ]
        
        # For market data update
        if data.get('type') == 'market':
            has_fields = all(field in data for field in market_fields)
            if not has_fields:
                missing = [field for field in market_fields if field not in data]
                self.fut_logger.debug(f"Missing market fields: {missing}")
            return has_fields
            
        # For depth data update
        if data.get('type') == 'depth':
            depth_fields = ['bid_price1', 'ask_price1', 'bid_size1', 'ask_size1']
            has_fields = all(field in data for field in depth_fields)
            if not has_fields:
                missing = [field for field in depth_fields if field not in data]
                self.fut_logger.debug(f"Missing depth fields: {missing}")
            return has_fields
            
        return False

    def insert_index_data(self, data, symbol):
        table_name = (
            symbol
            .replace(':', '_')
            .replace('-', '_')
            .replace('&', '_')
            .replace(' ', '_')
        )
        
        if not table_name[0].isalpha() and table_name[0] != '_':
            table_name = 'symbol_' + table_name
            
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(f"""
                    INSERT INTO {table_name} (
                        ltp, prev_close_price, ch, chp, exch_feed_time,
                        high_price, low_price, open_price, type
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                """, (
                    data.get('ltp'), data.get('prev_close_price'),
                    data.get('ch'), data.get('chp'),
                    data.get('exch_feed_time'), data.get('high_price'),
                    data.get('low_price'), data.get('open_price'),
                    data.get('type')
                ))
                print(f"Successfully inserted data for {symbol}")  # Debug log
        except Exception as e:
            print(f"Error inserting data for {symbol}: {str(e)}")  # Debug log

    def insert_fut_data(self, data, symbol):
        table_name = (
            symbol
            .replace(':', '_')
            .replace('-', '_')
            .replace('&', '_')
            .replace(' ', '_')
        )
        
        if not table_name[0].isalpha() and table_name[0] != '_':
            table_name = 'symbol_' + table_name
        
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
    database = index_fut
    """
    with open('api/ini/index_fut.ini', 'w') as f:
        f.write(ini_content)

    # Initialize managers
    symbol_manager = SymbolManager()
    db_manager = DatabaseManager('api/ini/index_fut.ini')

    # Get symbols and access token
    symbols = symbol_manager.read_symbol_list()
    access_token = read_access_token()

    # Separate index and futures symbols
    index_symbols = [sym for sym in symbols if sym.endswith('-INDEX')]
    futures_symbols = [sym for sym in symbols if sym.endswith('FUT')]

    # Create tables for each symbol
    for symbol in symbols:
        symbol_with_prefix = f"{symbol}"
        db_manager.create_table(symbol_with_prefix)

    def onmessage(message):
        """Handle incoming messages from the WebSocket."""
        try:
            symbol = message.get('symbol')
            msg_type = message.get('type')
            
            if not symbol or msg_type in ['cn', 'ful', 'sub']:
                return
            
            # Convert 'sf' type to 'market' and 'if' type to 'index'
            if msg_type == 'sf':
                message['type'] = 'market'
            elif msg_type == 'if':
                message['type'] = 'index'
            
            if symbol.endswith('-INDEX'):
                db_manager.update_cache_and_insert(message, symbol, 'market')
            elif msg_type in ['sf', 'dp']:
                update_type = 'market' if msg_type == 'sf' else 'depth'
                db_manager.update_cache_and_insert(message, symbol, update_type)
                
        except Exception as e:
            logging.error(f"Error in onmessage handler: {str(e)}", exc_info=True)

    def onopen():
        """Subscribe to data types and symbols upon WebSocket connection."""
        # Subscribe to SymbolUpdate for all symbols
        fyers.subscribe(symbols=[f"{sym}" for sym in symbols], data_type="SymbolUpdate")
        
        # Subscribe to DepthUpdate only for futures symbols
        if futures_symbols:
            fyers.subscribe(symbols=[f"{sym}" for sym in futures_symbols], data_type="DepthUpdate")
            
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