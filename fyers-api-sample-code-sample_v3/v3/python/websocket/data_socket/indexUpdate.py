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
    data_type = "SymbolUpdate"
    # data_type = "DepthUpdate"

    # Subscribe to the specified symbols and data type
    symbols = ["NSE:NIFTY50-INDEX"] # , "NSE:NIFTYBANK-INDEX"]
    # symbols = ["NSE:ACC25FEBFUT"]
    fyers.subscribe(symbols=symbols, data_type=data_type)

    # Keep the socket running to receive real-time data
    fyers.keep_running()


# Replace the sample access token with your actual access token obtained from Fyers
access_token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJhcGkuZnllcnMuaW4iLCJpYXQiOjE3NDAwMjU3OTQsImV4cCI6MTc0MDA5Nzg1NCwibmJmIjoxNzQwMDI1Nzk0LCJhdWQiOlsieDowIiwieDoxIiwieDoyIiwiZDoxIiwiZDoyIiwieDoxIiwieDowIiwieDoxIiwieDowIl0sInN1YiI6ImFjY2Vzc190b2tlbiIsImF0X2hhc2giOiJnQUFBQUFCbnRxX0NubGJNbWFhYWk3bThZUjB5ZnZLMHphUDgtb3Nza3Nqb3JfREdkNTlUV01fcGtQN05oTmpaY0RrTEdJbEpPUklIUnlHNklTS1o3bk42cHVudFBTWUNRNmtsdm96TTBybnlUQWhQbHJOc1R2MD0iLCJkaXNwbGF5X25hbWUiOiJWSUpBWSBLVU1BUiBTSEFSTUEiLCJvbXMiOiJLMSIsImhzbV9rZXkiOiI5MTdiZGFkMzI5YWE5YWQ0MjFkZjI4MDc3NmMzMmMxODcyYzFhOWE1NzBjYWNkZmEzYWUyODk3MyIsImlzRGRwaUVuYWJsZWQiOiJOIiwiaXNNdGZFbmFibGVkIjoiTiIsImZ5X2lkIjoiWVYwNzE5OSIsImFwcFR5cGUiOjEwMCwicG9hX2ZsYWciOiJOIn0.ildhkR7ai9CqDy-YZeDjwwPAxgnuD9Bd3BUYrDUK4ds"

# Create a FyersDataSocket instance with the provided parameters
fyers = data_ws.FyersDataSocket(
    access_token=access_token,       # Access token in the format "appid:accesstoken"
    log_path="",                     # Path to save logs. Leave empty to auto-create logs in the current directory.
    litemode=False,                  # Lite mode disabled. Set to True if you want a lite response.
    write_to_file=False,              # Save response in a log file instead of printing it.
    reconnect=True,                  # Enable auto-reconnection to WebSocket on disconnection.
    on_connect=onopen,               # Callback function to subscribe to data upon connection.
    on_close=onclose,                # Callback function to handle WebSocket connection close events.
    on_error=onerror,                # Callback function to handle WebSocket errors.
    on_message=onmessage             # Callback function to handle incoming messages from the WebSocket.
)

# Establish a connection to the Fyers WebSocket
fyers.connect()
