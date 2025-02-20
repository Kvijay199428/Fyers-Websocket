from fyers_apiv3.FyersWebsocket import data_ws


def onmessage(message):
    """
    Callback function to handle incoming messages from the FyersDataSocket WebSocket.

    Parameters:
        message (dict): The received message from the WebSocket.

    """
    print("Response:", message)


def onerror(message):
    """
    Callback function to handle WebSocket errors.

    Parameters:
        message (dict): The error message received from the WebSocket.


    """
    print("Error:", message)


def onclose(message):
    """
    Callback function to handle WebSocket connection close events.
    """
    print("Connection closed:", message)


def onopen():
    """
    Callback function to subscribe to data type and symbols upon WebSocket connection.

    """
    # Specify the data type and symbols you want to subscribe to
    # data_type = "SymbolUpdate"
    data_type = "DepthUpdate"


    # Subscribe to the specified symbols and data type
    symbols = ['NSE:NIFTY2522022950PE'] # ['NSE:SBIN-EQ', 'NSE:ADANIENT-EQ']
    fyers.subscribe(symbols=symbols, data_type=data_type)

    # Keep the socket running to receive real-time data
    fyers.keep_running()


# Replace the sample access token with your actual access token obtained from Fyers
access_token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJhcGkuZnllcnMuaW4iLCJpYXQiOjE3Mzk1ODI4NjQsImV4cCI6MTczOTY2NTg0NCwibmJmIjoxNzM5NTgyODY0LCJhdWQiOlsieDowIiwieDoxIiwieDoyIiwiZDoxIiwiZDoyIiwieDoxIiwieDowIiwieDoxIiwieDowIl0sInN1YiI6ImFjY2Vzc190b2tlbiIsImF0X2hhc2giOiJnQUFBQUFCbnItMlFKSU51VENTQUxDQXotTzdYemtGZGRZQzRrbW9oMHNDbkdjcGs5dXdEeWJyOC02bjVSZlQ3eGdlRm5nOWI1MGNja3dzZDRGWjNLOUp4M3pLRk1ITUVJNlQ1eEk0UVBrX3QwSXRuZHRZbkJJQT0iLCJkaXNwbGF5X25hbWUiOiJWSUpBWSBLVU1BUiBTSEFSTUEiLCJvbXMiOiJLMSIsImhzbV9rZXkiOiI5MTdiZGFkMzI5YWE5YWQ0MjFkZjI4MDc3NmMzMmMxODcyYzFhOWE1NzBjYWNkZmEzYWUyODk3MyIsImlzRGRwaUVuYWJsZWQiOiJOIiwiaXNNdGZFbmFibGVkIjoiTiIsImZ5X2lkIjoiWVYwNzE5OSIsImFwcFR5cGUiOjEwMCwicG9hX2ZsYWciOiJOIn0.HWxKMmsw908Ph59NuEY-5op-Kf26t59cAHYc_X6wpL8"

# Create a FyersDataSocket instance with the provided parameters
fyers = data_ws.FyersDataSocket(
    access_token=access_token,       # Access token in the format "appid:accesstoken"
    log_path="",                     # Path to save logs. Leave empty to auto-create logs in the current directory.
    litemode=False,                  # Lite mode disabled. Set to True if you want a lite response.
    write_to_file=True,              # Save response in a log file instead of printing it.
    reconnect=True,                  # Enable auto-reconnection to WebSocket on disconnection.
    on_connect=onopen,               # Callback function to subscribe to data upon connection.
    on_close=onclose,                # Callback function to handle WebSocket connection close events.
    on_error=onerror,                # Callback function to handle WebSocket errors.
    on_message=onmessage             # Callback function to handle incoming messages from the WebSocket.
)

# Establish a connection to the Fyers WebSocket
fyers.connect()
