# historical.py
import os
import asyncio
import aiohttp
import tarfile
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv
from fyers_apiv3 import fyersModel
from datetime import datetime, timedelta
import json
import polars as pl
import time
from pathlib import Path
import requests
import pytz

class AsyncHistoricalDataFetcher:
    def __init__(self, max_workers=5):
        load_dotenv()
        self.client_id = os.getenv('APP_ID')
        if not self.client_id:
            raise Exception("APP_ID not found in .env file")
        self.access_token = self._load_access_token()
        self.max_workers = max_workers
        self.ist_tz = pytz.timezone('Asia/Kolkata')
        
    def _load_access_token(self):
        """Load access token from file"""
        try:
            with open('api/token/access_token', 'r') as file:
                return file.read().strip()
        except FileNotFoundError:
            raise Exception("Access token file not found at 'api/token/access_token'")
        except Exception as e:
            raise Exception(f"Error reading access token: {str(e)}")

    def download_symbol_file(self):
        """Download the symbol master file"""
        url = "https://public.fyers.in/sym_details/NSE_FO_sym_master.json"
        local_file = "NSE_FO_sym_master.json"
        
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
            
            with open(local_file, 'w', encoding='utf-8') as f:
                f.write(content)
            
            print("Symbol master file downloaded and cleaned successfully")
            return local_file
            
        except Exception as e:
            raise Exception(f"Error downloading symbol file: {str(e)}")

    def read_symbol_list(self):
        try:
            local_file = self.download_symbol_file()
            with open(local_file, 'r') as f:
                data = json.load(f)
            option_symbols = [
                symbol for symbol in data.keys() 
                if (symbol.endswith('CE') or symbol.endswith('PE')) and symbol.startswith('NSE:')
            ]
            option_symbols.sort()
            print(f"Found {len(option_symbols)} option symbols")
            if option_symbols:
                print("Sample CE symbols:", [s for s in option_symbols[:5] if s.endswith('CE')])
                print("Sample PE symbols:", [s for s in option_symbols[:5] if s.endswith('PE')])
            return option_symbols
        except Exception as e:
            raise Exception(f"Error reading symbol list: {str(e)}")
    def get_data_chunk(self, symbol, start_date, end_date):
        """Fetch data for a specific date range with authentication error handling"""
        fyers = fyersModel.FyersModel(
            client_id=self.client_id,
            is_async=False,
            token=self.access_token,
            log_path=""
        )
        
        end_date = end_date - timedelta(minutes=1)
        from_timestamp = int(start_date.timestamp())
        to_timestamp = int(end_date.timestamp())
        
        data = {
            "symbol": symbol,
            "resolution": "1",      # 1 minute candle
            "date_format": "0",
            "range_from": str(from_timestamp),
            "range_to": str(to_timestamp),
            "cont_flag": "1"
        }
        
        try:
            response = fyers.history(data=data)
            if isinstance(response, dict):
                if response.get('s') == 'ok':
                    return response.get('candles', [])
                elif response.get('code') == -16 or "authenticate" in str(response.get('message', '')).lower():
                    print("\nAuthentication error detected!")
                    print("Please login using the token generation script.")
                    input("After logging in, press Enter to continue...")
                    
                    # Reload access token and retry
                    self.access_token = self._load_access_token()
                    fyers = fyersModel.FyersModel(
                        client_id=self.client_id,
                        is_async=False,
                        token=self.access_token,
                        log_path=""
                    )
                    response = fyers.history(data=data)
                    
                    if isinstance(response, dict) and response.get('s') == 'ok':
                        return response.get('candles', [])
                    else:
                        raise Exception(f"API Error after reauth: {json.dumps(response, indent=2)}")
                else:
                    raise Exception(f"API Error: {json.dumps(response, indent=2)}")
            else:
                raise Exception(f"Unexpected response format: {response}")
                
        except Exception as e:
            raise Exception(f"Error fetching data chunk: {str(e)}")
    async def process_symbol(self, symbol, years=1):  # Changed default to 1 year for options
        """Process a single symbol with authentication error handling"""
        print(f"Starting processing for {symbol}")
        
        # For options, we'll limit the date range to a more reasonable period
        end_date = datetime.now()
        start_date = end_date - timedelta(days=years * 365)
        
        # Extract option details from symbol
        # Example: NSE:NIFTY2522028650CE -> Strike: 28650, Expiry: 25220
        try:
            parts = symbol.split(':')[1]  # Remove NSE: prefix
            instrument = ''.join(filter(str.isalpha, parts[:5]))  # Get instrument name (e.g., NIFTY)
            expiry = parts[5:10]  # Get expiry date
            strike = parts[10:-2]  # Get strike price
            option_type = parts[-2:]  # Get option type (CE/PE)
            
            print(f"Processing {instrument} {expiry} {strike} {option_type}")
        except Exception as e:
            print(f"Error parsing symbol {symbol}: {str(e)}")
        
        all_data = []
        chunk_size = timedelta(days=30)  # Smaller chunk size for options
        current_start = start_date
        
        symbol_dir = Path('historicalDataOptions')
        symbol_dir.mkdir(exist_ok=True)
        
        while current_start < end_date:
            current_end = min(current_start + chunk_size, end_date)
            print(f"{symbol}: Fetching data from {current_start} to {current_end}")
            
            try:
                loop = asyncio.get_event_loop()
                with ThreadPoolExecutor() as pool:
                    chunk_data = await loop.run_in_executor(
                        pool, 
                        self.get_data_chunk, 
                        symbol, 
                        current_start, 
                        current_end
                    )
                
                if chunk_data:
                    all_data.extend(chunk_data)
                await asyncio.sleep(1)  # Rate limiting
                
            except Exception as e:
                print(f"{symbol}: Error processing chunk {current_start} to {current_end}: {str(e)}")
                if "authenticate" in str(e).lower():
                    continue  # Retry the same chunk after reauth
                
            current_start = current_end
        
        try:
            if all_data:
                # Create Polars DataFrame with explicit orientation
                df = pl.DataFrame(
                    all_data,
                    schema=[
                        ("timestamp", pl.Int64),
                        ("open", pl.Float64),
                        ("high", pl.Float64),
                        ("low", pl.Float64),
                        ("close", pl.Float64),
                        ("volume", pl.Float64)
                    ],
                    orient="row"
                )
                
                # Convert timestamp to datetime with proper timezone handling
                df = df.with_columns([
                    pl.col("timestamp")
                    .map_elements(
                        lambda x: datetime.fromtimestamp(x)
                        .astimezone(pytz.timezone('Asia/Kolkata'))
                        .strftime('%Y-%m-%d %H:%M'),
                        return_dtype=pl.Utf8
                    )
                    .str.strptime(pl.Datetime, '%Y-%m-%d %H:%M')
                    .alias("timestamp")  # Keep the original column name
                ])

                # Ensure timezone is set to IST
                df = df.with_columns([
                    pl.col("timestamp")
                    .dt.replace_time_zone("Asia/Kolkata")
                ])

                # Sort by timestamp
                df = df.sort("timestamp")
                
                # Drop original timestamp column
                # df = df.drop("timestamp")
                
                # Create temporary CSV file
                temp_csv = f"temp_{symbol.replace(':', '_')}.csv"
                df.write_csv(temp_csv)
                
                # Create tar.gz file
                start_str = start_date.strftime('%Y-%m-%d')
                end_str = end_date.strftime('%Y-%m-%d')
                # archive_name = f"{symbol.replace(':', '_')}_{start_str}_{end_str}.tar.gz"
                archive_name = f"{symbol.replace(':', '_')}.tar.gz"
                
                # Compress CSV to tar.gz
                with tarfile.open(symbol_dir / archive_name, 'w:gz') as tar:
                    tar.add(temp_csv, arcname=f"{symbol.replace(':', '_')}.csv")
                
                # Remove temporary CSV file
                os.remove(temp_csv)
                
                print(f"{symbol}: Saved complete {years}-year data from {start_str} to {end_str}")
        
        except Exception as e:
            print(f"{symbol}: Error saving data: {str(e)}")
        
        return symbol

    def compress_results(self):
        """Compress all CSV files into a tar.gz archive"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        archive_name = f'historical_data_{timestamp}.tar.gz'
        
        with tarfile.open(archive_name, 'w:gz') as tar:
            for csv_file in self.output_dir.glob('*.csv'):
                tar.add(csv_file, arcname=csv_file.name)
        
        return archive_name

    async def process_all_symbols(self, years=1):
        """Process all equity symbols"""
        try:
            symbols = self.read_symbol_list()
            print(f"Processing {len(symbols)} equity symbols")
            
            semaphore = asyncio.Semaphore(self.max_workers)
            
            async def process_with_semaphore(symbol):
                async with semaphore:
                    return await self.process_symbol(symbol, years)
            
            tasks = [process_with_semaphore(symbol) for symbol in symbols]
            await asyncio.gather(*tasks)
            
            print(f"All data saved in historicalData directory")
            
        except Exception as e:
            print(f"Error in main processing: {str(e)}")

async def main():
    try:
        fetcher = AsyncHistoricalDataFetcher(max_workers=5)
        await fetcher.process_all_symbols()
        
    except Exception as e:
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    asyncio.run(main())