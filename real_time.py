import requests
import json
# from earthquake_script import get_data_real_time  to_parquet  parquet_file_path_real_time,flatten_json
from earthquake_module import DataExt
import pandas as pd
from datetime import datetime 
from datetime import timedelta 
import time
from p import query_params
from datetime import timezone
import pytz
import calendar
URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"
print(query_params)
# currentStTime = datetime.now()
# currentStTime = currentStTime.astimezone(pytz.utc) - timedelta(minutes=50)
# currentEnTime = currentStTime + timedelta(minutes=5)
# https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime=2002-01-01T00:00:00&endtime=2003-01-01T00:00:00
# # print(newtime)
# query_params= {
#     "format":"geojson",
#     "starttime":currentStTime,
#     "endtime":currentEnTime,
#     "minmagnitude":1.0
# }
# def get_data(query_params):
#     response = requests.get(URL,query_params)
#     d =response.json()
#     return d
# print(dat)
# l=flatten_json(data=dat)
# to_parquet(data=l,filepath=parquet_file_path)
# parquet_file_path_real_time()
# print("done")
while(True):
    count_of_data = DataExt.get_data_real_time(query_params=query_params).get("metadata",[]).get("count")
    prevStartTime = query_params.get("starttime")
    prevStartTime=pd.to_datetime(prevStartTime)
    prevEndTime = query_params.get("endtime")
    prevEndTime = pd.to_datetime(prevEndTime)
    year= prevEndTime.year
    if count_of_data==0:

        print(f"No data available for from range {query_params.get("starttime")} and {query_params.get("endtime")}")
        newStartTime = prevEndTime
        if calendar.isleap(year=year):
            newEndTime = prevEndTime + timedelta(days=366)
        else:
            newEndTime = prevEndTime + timedelta(days=365)
        query_params["starttime"]=newStartTime        
        query_params["endtime"] = newEndTime
    else:
        print(f"Data for range from {query_params.get("starttime")} and {query_params.get("endtime")} is being processed")
        # data = get_data(query_params=query_params)
        data = DataExt.get_data_real_time(query_params=query_params)
        l=DataExt.flatten_json(data=data)
        DataExt.parquet_file_path_real_time()
        # print(data)
        newStartTime = prevEndTime
        if calendar.isleap(year=year):
            newEndTime = prevEndTime + timedelta(days=366)
        else:
            newEndTime = prevEndTime + timedelta(days=365)
        query_params["starttime"]=newStartTime        
        query_params["endtime"] = newEndTime

    # time.sleep(60*60)
    time.sleep(5)

# print(newDateTime)