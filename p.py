import requests
import json
# from earthquake_script import get_data, to_parquet, parquet_file_path_real_time,flatten_json
import pandas as pd
from datetime import datetime 
from datetime import timedelta 
import time
from datetime import timezone
import pytz
# query_params = {
#     "format" : "geojson",
#     "minmagnitude":2.0,
#     "starttime": "1940-01-01",
#     "endtime":"1941-01-01",
#     "eventtype":"earthquake",
#     "minlatitude":8.0,
#     "maxlatitude":37.0,
#     "minlongitude":68.0,
#     "maxlongitude":97.0
# }
# currentStTime = datetime.now()
# currentStTime = currentStTime.astimezone(pytz.utc) - timedelta(minutes=45)

# print(newtime)
query_params= {
    "format":"geojson",
    # "starttime":currentStTime,
    "starttime":'1940-01-01',
    "endtime":'1941-01-01',
    "minmagnitude":2.5
}
# currentEnTime = query_params["starttime"] + timedelta(days=10)

# newtime = datetime.now()
# print(newtime)
# newtime = newtime.astimezone(pytz.utc)

# print(newtime)