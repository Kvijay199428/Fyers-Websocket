# # historical.py
# import os
# import asyncio
# import aiohttp
# import tarfile
# from concurrent.futures import ThreadPoolExecutor
# from dotenv import load_dotenv
# from fyers_apiv3 import fyersModel
# from datetime import datetime, timedelta
# import json
# import polars as pl
# import time
# from pathlib import Path
# import requests
# import pytz

# class AsyncHistoricalDataFetcher:
#     def __init__(self, max_workers=5):
#         load_dotenv()
#         self.client_id = os.getenv('APP_ID')
#         if not self.client_id:
#             raise Exception("APP_ID not found in .env file")
#         self.access_token = self._load_access_token()
#         self.max_workers = max_workers
#         self.ist_tz = pytz.timezone('Asia/Kolkata')
        
#     def _load_access_token(self):
#         """Load access token from file"""
#         try:
#             with open('api/token/access_token', 'r') as file:
#                 return file.read().strip()
#         except FileNotFoundError:
#             raise Exception("Access token file not found at 'api/token/access_token'")
#         except Exception as e:
#             raise Exception(f"Error reading access token: {str(e)}")

#     def download_symbol_file(self):
#         """Download the symbol master file"""
#         url = "https://public.fyers.in/sym_details/NSE_CM_sym_master.json"
#         local_file = "NSE_CM_sym_master.json"
        
#         try:
#             print("Downloading symbol master file...")
#             response = requests.get(url)
#             response.raise_for_status()
            
#             content = response.text
#             content = content.strip()
#             if content.endswith(','):
#                 content = content[:-1]
            
#             if not content.startswith('{'):
#                 content = '{' + content
#             if not content.endswith('}'):
#                 content = content + '}'
            
#             with open(local_file, 'w', encoding='utf-8') as f:
#                 f.write(content)
            
#             print("Symbol master file downloaded and cleaned successfully")
#             return local_file
            
#         except Exception as e:
#             raise Exception(f"Error downloading symbol file: {str(e)}")

#     def read_symbol_list(self):
#         """Read and parse the local symbol master file"""
#         try:
#             local_file = self.download_symbol_file()
            
#             with open(local_file, 'r') as f:
#                 data = json.load(f)
            
#             equity_symbols = [symbol for symbol in data.keys() if symbol.endswith('-EQ')]
            
#             print(f"Found {len(equity_symbols)} equity symbols")
#             if equity_symbols:
#                 print("Sample symbols:", equity_symbols[:5])
            
#             return equity_symbols
            
#         except Exception as e:
#             raise Exception(f"Error reading symbol list: {str(e)}")
#     def get_data_chunk(self, symbol, start_date, end_date):
#         """Fetch data for a specific date range with authentication error handling"""
#         fyers = fyersModel.FyersModel(
#             client_id=self.client_id,
#             is_async=False,
#             token=self.access_token,
#             log_path=""
#         )
        
#         end_date = end_date - timedelta(minutes=1)
#         from_timestamp = int(start_date.timestamp())
#         to_timestamp = int(end_date.timestamp())
        
#         data = {
#             "symbol": symbol,
#             "resolution": "1",      # 1 minute candle
#             "date_format": "0",
#             "range_from": str(from_timestamp),
#             "range_to": str(to_timestamp),
#             "cont_flag": "1"
#         }
        
#         try:
#             response = fyers.history(data=data)
#             if isinstance(response, dict):
#                 if response.get('s') == 'ok':
#                     return response.get('candles', [])
#                 elif response.get('code') == -16 or "authenticate" in str(response.get('message', '')).lower():
#                     print("\nAuthentication error detected!")
#                     print("Please login using the token generation script.")
#                     input("After logging in, press Enter to continue...")
                    
#                     # Reload access token and retry
#                     self.access_token = self._load_access_token()
#                     fyers = fyersModel.FyersModel(
#                         client_id=self.client_id,
#                         is_async=False,
#                         token=self.access_token,
#                         log_path=""
#                     )
#                     response = fyers.history(data=data)
                    
#                     if isinstance(response, dict) and response.get('s') == 'ok':
#                         return response.get('candles', [])
#                     else:
#                         raise Exception(f"API Error after reauth: {json.dumps(response, indent=2)}")
#                 else:
#                     raise Exception(f"API Error: {json.dumps(response, indent=2)}")
#             else:
#                 raise Exception(f"Unexpected response format: {response}")
                
#         except Exception as e:
#             raise Exception(f"Error fetching data chunk: {str(e)}")
#     async def process_symbol(self, symbol, years=10):
#         """Process a single symbol with authentication error handling"""
#         print(f"Starting processing for {symbol}")
#         end_date = datetime.now()
#         start_date = end_date - timedelta(days=years * 365)
        
#         all_data = []
#         chunk_size = timedelta(days=100)
#         current_start = start_date
        
#         symbol_dir = Path('historicalData')
#         symbol_dir.mkdir(exist_ok=True)
        
#         while current_start < end_date:
#             current_end = min(current_start + chunk_size, end_date)
#             print(f"{symbol}: Fetching data from {current_start} to {current_end}")
            
#             try:
#                 loop = asyncio.get_event_loop()
#                 with ThreadPoolExecutor() as pool:
#                     chunk_data = await loop.run_in_executor(
#                         pool, 
#                         self.get_data_chunk, 
#                         symbol, 
#                         current_start, 
#                         current_end
#                     )
                
#                 if chunk_data:
#                     all_data.extend(chunk_data)
#                 await asyncio.sleep(1)  # Rate limiting
                
#             except Exception as e:
#                 print(f"{symbol}: Error processing chunk {current_start} to {current_end}: {str(e)}")
#                 if "authenticate" in str(e).lower():
#                     continue  # Retry the same chunk after reauth
                
#             current_start = current_end
        
#         try:
#             if all_data:
#                 # Create Polars DataFrame with explicit orientation
#                 df = pl.DataFrame(
#                     all_data,
#                     schema=[
#                         ("timestamp", pl.Int64),
#                         ("open", pl.Float64),
#                         ("high", pl.Float64),
#                         ("low", pl.Float64),
#                         ("close", pl.Float64),
#                         ("volume", pl.Float64)
#                     ],
#                     orient="row"
#                 )
                
#                 # Convert timestamp to datetime with proper timezone handling
#                 df = df.with_columns([
#                     pl.col("timestamp")
#                     .map_elements(
#                         lambda x: datetime.fromtimestamp(x)
#                         .astimezone(pytz.timezone('Asia/Kolkata'))
#                         .strftime('%Y-%m-%d %H:%M'),
#                         return_dtype=pl.Utf8
#                     )
#                     .str.strptime(pl.Datetime, '%Y-%m-%d %H:%M')
#                     .alias("timestamp")  # Keep the original column name
#                 ])

#                 # Ensure timezone is set to IST
#                 df = df.with_columns([
#                     pl.col("timestamp")
#                     .dt.replace_time_zone("Asia/Kolkata")
#                 ])

#                 # Sort by timestamp
#                 df = df.sort("timestamp")
                
#                 # Drop original timestamp column
#                 # df = df.drop("timestamp")
                
#                 # Create temporary CSV file
#                 temp_csv = f"temp_{symbol.replace(':', '_')}.csv"
#                 df.write_csv(temp_csv)
                
#                 # Create tar.gz file
#                 start_str = start_date.strftime('%Y-%m-%d')
#                 end_str = end_date.strftime('%Y-%m-%d')
#                 # archive_name = f"{symbol.replace(':', '_')}_{start_str}_{end_str}.tar.gz"
#                 archive_name = f"{symbol.replace(':', '_')}.tar.gz"
                
#                 # Compress CSV to tar.gz
#                 with tarfile.open(symbol_dir / archive_name, 'w:gz') as tar:
#                     tar.add(temp_csv, arcname=f"{symbol.replace(':', '_')}.csv")
                
#                 # Remove temporary CSV file
#                 os.remove(temp_csv)
                
#                 print(f"{symbol}: Saved complete {years}-year data from {start_str} to {end_str}")
        
#         except Exception as e:
#             print(f"{symbol}: Error saving data: {str(e)}")
        
#         return symbol

#     def compress_results(self):
#         """Compress all CSV files into a tar.gz archive"""
#         timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
#         archive_name = f'historical_data_{timestamp}.tar.gz'
        
#         with tarfile.open(archive_name, 'w:gz') as tar:
#             for csv_file in self.output_dir.glob('*.csv'):
#                 tar.add(csv_file, arcname=csv_file.name)
        
#         return archive_name

#     async def process_all_symbols(self, years=10):
#         """Process all equity symbols"""
#         try:
#             symbols = self.read_symbol_list()
#             print(f"Processing {len(symbols)} equity symbols")
            
#             semaphore = asyncio.Semaphore(self.max_workers)
            
#             async def process_with_semaphore(symbol):
#                 async with semaphore:
#                     return await self.process_symbol(symbol, years)
            
#             tasks = [process_with_semaphore(symbol) for symbol in symbols]
#             await asyncio.gather(*tasks)
            
#             print(f"All data saved in historicalData directory")
            
#         except Exception as e:
#             print(f"Error in main processing: {str(e)}")

# async def main():
#     try:
#         fetcher = AsyncHistoricalDataFetcher(max_workers=5)
#         await fetcher.process_all_symbols()
        
#     except Exception as e:
#         print(f"Error: {str(e)}")

# if __name__ == "__main__":
#     asyncio.run(main())

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
import shutil

class AsyncHistoricalDataFetcher:
    def __init__(self, max_workers=5):
        load_dotenv()
        self.client_id = os.getenv('APP_ID')
        if not self.client_id:
            raise Exception("APP_ID not found in .env file")
        self.access_token = self._load_access_token()
        self.max_workers = max_workers
        self.ist_tz = pytz.timezone('Asia/Kolkata')
        self.data_dir = Path('historicalData')
        self.data_dir.mkdir(exist_ok=True)
        self.log_dir = Path('logs')
        self.log_dir.mkdir(exist_ok=True)
        
    def _load_access_token(self):
        """Load access token from file"""
        try:
            with open('api/token/access_token', 'r') as file:
                return file.read().strip()
        except FileNotFoundError:
            raise Exception("Access token file not found at 'api/token/access_token'")
        except Exception as e:
            raise Exception(f"Error reading access token: {str(e)}")

    def _clean_temp_dir(self, temp_dir):
        """Safely clean up temporary directory"""
        try:
            if temp_dir.exists():
                shutil.rmtree(temp_dir, ignore_errors=True)
        except Exception as e:
            print(f"Warning: Could not clean temporary directory: {str(e)}")

    def get_last_timestamp(self, symbol):
        """Get the last timestamp from existing data for a symbol with robust error handling"""
        temp_dir = Path('temp_extract')
        archive_path = self.data_dir / f"{symbol.replace(':', '_')}.tar.gz"
        
        try:
            # Clean up any existing temp directory first
            self._clean_temp_dir(temp_dir)
            
            if not archive_path.exists():
                return None

            # Create a fresh temporary directory
            temp_dir.mkdir(exist_ok=True)
            
            try:
                # Try to read and validate the archive
                with tarfile.open(archive_path, 'r:gz') as tar:
                    csv_name = f"{symbol.replace(':', '_')}.csv"
                    try:
                        tar.extract(csv_name, temp_dir, filter='data')
                    except (tarfile.TarError, OSError) as e:
                        print(f"Warning: Corrupted archive detected for {symbol}, removing and starting fresh")
                        archive_path.unlink()  # Remove corrupted file
                        return None
                    
                    try:
                        # Read the last row of the CSV using Polars
                        df = pl.read_csv(temp_dir / csv_name)
                        if len(df) > 0:
                            last_timestamp = df['timestamp'].max()
                            return datetime.strptime(str(last_timestamp)[:10], '%Y-%m-%d')
                    except pl.exceptions.ComputeError as e:
                        print(f"Warning: Data format error in CSV for {symbol}, removing and starting fresh")
                        archive_path.unlink()
                        return None
                    
            except tarfile.ReadError as e:
                print(f"Warning: Invalid tar.gz file for {symbol}, removing and starting fresh")
                archive_path.unlink()
                return None
                
            return None
            
        except Exception as e:
            print(f"Error reading last timestamp for {symbol}: {str(e)}")
            # In case of unexpected errors, we'll also remove the potentially corrupted file
            try:
                archive_path.unlink()
            except:
                pass
            return None
            
        finally:
            # Always clean up the temporary directory
            self._clean_temp_dir(temp_dir)

    def download_symbol_file(self):
        """Download the symbol master file"""
        url = "https://public.fyers.in/sym_details/NSE_CM_sym_master.json"
        local_file = "NSE_CM_sym_master.json"
        
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

    async def get_data_chunk(self, symbol, start_date, end_date):
        """Fetch data for a specific date range with proper async handling"""
        fyers = fyersModel.FyersModel(
            client_id=self.client_id,
            is_async=True,  # Keep async=True
            token=self.access_token,
            log_path=str(self.log_dir)
        )
        
        end_date = end_date - timedelta(minutes=1)
        from_timestamp = int(start_date.timestamp())
        to_timestamp = int(end_date.timestamp())
        
        data = {
            "symbol": symbol,
            "resolution": "1",
            "date_format": "0",
            "range_from": str(from_timestamp),
            "range_to": str(to_timestamp),
            "cont_flag": "1"
        }
        
        try:
            # Properly await the async call
            response = await fyers.history(data=data)
            if isinstance(response, dict):
                if response.get('s') == 'ok':
                    return response.get('candles', [])
                elif response.get('code') == -16 or "authenticate" in str(response.get('message', '')).lower():
                    print(f"\n{symbol}: Authentication error detected!")
                    print("Please login using the token generation script.")
                    input("After logging in, press Enter to continue...")
                    
                    self.access_token = self._load_access_token()
                    fyers = fyersModel.FyersModel(
                        client_id=self.client_id,
                        is_async=True,
                        token=self.access_token,
                        log_path=str(self.log_dir)
                    )
                    response = await fyers.history(data=data)
                    
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

    async def process_symbol(self, symbol, years=10):
        """Process a single symbol with async data fetching"""
        print(f"Starting processing for {symbol}")
        temp_dir = Path('temp_extract')
        
        try:
            last_timestamp = self.get_last_timestamp(symbol)
            end_date = datetime.now()
            
            if last_timestamp:
                print(f"{symbol}: Found existing data up to {last_timestamp.date()}")
                start_date = last_timestamp + timedelta(days=1)
                if start_date >= end_date:
                    print(f"{symbol}: Data is already up to date")
                    return symbol
            else:
                print(f"{symbol}: No existing data found, fetching {years} years of historical data")
                start_date = end_date - timedelta(days=years * 365)
            
            all_data = []
            chunk_size = timedelta(days=100)
            current_start = start_date
            
            while current_start < end_date:
                current_end = min(current_start + chunk_size, end_date)
                print(f"{symbol}: Fetching data from {current_start} to {current_end}")
                
                try:
                    # Directly await the async function
                    chunk_data = await self.get_data_chunk(symbol, current_start, current_end)
                    
                    if chunk_data:
                        all_data.extend(chunk_data)
                    await asyncio.sleep(1)  # Rate limiting
                    
                except Exception as e:
                    print(f"{symbol}: Error processing chunk {current_start} to {current_end}: {str(e)}")
                    if "authenticate" in str(e).lower():
                        continue  # Retry the same chunk after reauth
                    
                current_start = current_end
            
            if all_data:
                # Clean up any existing temp directory
                self._clean_temp_dir(temp_dir)
                temp_dir.mkdir(exist_ok=True)
                
                # Create new DataFrame
                new_df = pl.DataFrame(
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
                
                # Convert Unix timestamp to datetime string with timezone
                new_df = new_df.with_columns([
                    pl.col("timestamp")
                    .map_elements(
                        lambda x: datetime.fromtimestamp(x)
                        .astimezone(pytz.timezone('Asia/Kolkata'))
                        .strftime('%Y-%m-%dT%H:%M:%S.000000+0530'),  # Match the existing format
                        return_dtype=pl.Utf8
                    )
                    .alias("timestamp")
                ])

                # Sort by timestamp
                new_df = new_df.sort("timestamp")
                
                # If we have existing data, merge it with the new data
                if last_timestamp:
                    archive_path = self.data_dir / f"{symbol.replace(':', '_')}.tar.gz"
                    
                    with tarfile.open(archive_path, 'r:gz') as tar:
                        csv_name = f"{symbol.replace(':', '_')}.csv"
                        tar.extract(csv_name, temp_dir, filter='data')
                        
                        # Read existing data without parsing dates
                        existing_df = pl.read_csv(
                            temp_dir / csv_name,
                            try_parse_dates=False
                        )
                        
                        # Combine existing and new data
                        combined_df = pl.concat([existing_df, new_df])
                        combined_df = combined_df.unique(subset=["timestamp"])
                        combined_df = combined_df.sort("timestamp")
                        
                        # No need to parse and reformat timestamps since they're already in the correct format
                        new_df = combined_df
                
                # Create temporary CSV file
                temp_csv = temp_dir / f"temp_{symbol.replace(':', '_')}.csv"
                new_df.write_csv(temp_csv)
                
                # Create tar.gz file
                archive_name = f"{symbol.replace(':', '_')}.tar.gz"
                
                with tarfile.open(self.data_dir / archive_name, 'w:gz') as tar:
                    tar.add(temp_csv, arcname=f"{symbol.replace(':', '_')}.csv")
                
                print(f"{symbol}: Data successfully {'updated' if last_timestamp else 'saved'}")
                
        except Exception as e:
            print(f"{symbol}: Error processing symbol: {str(e)}")
        finally:
            self._clean_temp_dir(temp_dir)
        
        return symbol

    async def process_all_symbols(self, years=10):
        """Process all equity symbols with prioritization"""
        try:
            symbols = self.read_symbol_list()
            print(f"Processing {len(symbols)} equity symbols")
            
            # Separate symbols into two groups: those without data and those with data
            symbols_without_data = []
            symbols_with_data = []
            
            for symbol in symbols:
                last_timestamp = self.get_last_timestamp(symbol)
                if last_timestamp is None:
                    symbols_without_data.append(symbol)
                else:
                    symbols_with_data.append(symbol)
            
            print(f"Found {len(symbols_without_data)} symbols without data")
            print(f"Found {len(symbols_with_data)} symbols with existing data")
            
            # Process symbols without data first
            if symbols_without_data:
                print("\nProcessing symbols without historical data...")
                semaphore = asyncio.Semaphore(self.max_workers)
                
                async def process_with_semaphore(symbol):
                    async with semaphore:
                        return await self.process_symbol(symbol, years)
                
                tasks = [process_with_semaphore(symbol) for symbol in symbols_without_data]
                await asyncio.gather(*tasks)
            
            # Then process symbols that need updates
            if symbols_with_data:
                print("\nUpdating symbols with existing data...")
                tasks = [process_with_semaphore(symbol) for symbol in symbols_with_data]
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