from fyers_apiv3 import fyersModel
from dotenv import load_dotenv
import os
load_dotenv()

client_id = os.getenv("client_id")
access_token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJhcGkuZnllcnMuaW4iLCJpYXQiOjE3NDAwMjU3OTQsImV4cCI6MTc0MDA5Nzg1NCwibmJmIjoxNzQwMDI1Nzk0LCJhdWQiOlsieDowIiwieDoxIiwieDoyIiwiZDoxIiwiZDoyIiwieDoxIiwieDowIiwieDoxIiwieDowIl0sInN1YiI6ImFjY2Vzc190b2tlbiIsImF0X2hhc2giOiJnQUFBQUFCbnRxX0NubGJNbWFhYWk3bThZUjB5ZnZLMHphUDgtb3Nza3Nqb3JfREdkNTlUV01fcGtQN05oTmpaY0RrTEdJbEpPUklIUnlHNklTS1o3bk42cHVudFBTWUNRNmtsdm96TTBybnlUQWhQbHJOc1R2MD0iLCJkaXNwbGF5X25hbWUiOiJWSUpBWSBLVU1BUiBTSEFSTUEiLCJvbXMiOiJLMSIsImhzbV9rZXkiOiI5MTdiZGFkMzI5YWE5YWQ0MjFkZjI4MDc3NmMzMmMxODcyYzFhOWE1NzBjYWNkZmEzYWUyODk3MyIsImlzRGRwaUVuYWJsZWQiOiJOIiwiaXNNdGZFbmFibGVkIjoiTiIsImZ5X2lkIjoiWVYwNzE5OSIsImFwcFR5cGUiOjEwMCwicG9hX2ZsYWciOiJOIn0.ildhkR7ai9CqDy-YZeDjwwPAxgnuD9Bd3BUYrDUK4ds"

# Initialize the FyersModel instance with your client_id, access_token, and enable async mode
fyers = fyersModel.FyersModel(client_id=client_id, is_async=False, token=access_token, log_path="")

data = {
    "symbol":"NSE:SBIN-EQ",
    "resolution":"D",
    "date_format":"0",
    "range_from":"1688389716",
    "range_to":"1691068173",
    "cont_flag":"1"
}

response = fyers.history(data=data)
print(response)



