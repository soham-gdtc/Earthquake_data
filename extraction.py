import requests
import json
from earthquake_module import DataExt
# from earthquake_module import get_data_real_time ,to_parquet, parquet_file_path_real_time, flatten_json
import pandas as pd
from datetime import datetime 
from datetime import timedelta 
import time
from p import query_params
from datetime import timezone
import pytz
import logging

URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"

logging.basicConfig(filename="real_time.log",
                    format='%(asctime)s %(message)s',
                    filemode='w', level=logging.INFO)

logger = logging.getLogger()

while(True):
    currentTime = datetime.now()
    currentTime = currentTime.astimezone(pytz.utc)
    count_of_data = DataExt.get_data_real_time(query_params=query_params).get("metadata",[]).get("count")
    prevStartTime = query_params.get("starttime")
    prevStartTime=pd.to_datetime(prevStartTime)
    prevEndTime = query_params.get("endtime")
    prevEndTime = pd.to_datetime(prevEndTime)
    # if currentTime<prevEndTime:
    #     print(currentTime,prevEndTime)
    #     s1 = currentTime.timestamp()
    #     s2 = prevEndTime.timestamp()
    #     print(f"current time is {currentTime} and previous time was {prevEndTime} difference is {round((s2-s1),2)} seconds")
    #     time.sleep(s2-s1)
    if count_of_data==0:
        # print(f"No data available for from range {query_params.get("starttime")} and {query_params.get("endtime")}")
        logger.warning(f"No data available for from range {query_params.get("starttime")} and {query_params.get("endtime")}")
        newStartTime = prevEndTime
        newEndTime = prevEndTime + timedelta(days=10)
        query_params["starttime"]=newStartTime        
        query_params["endtime"] = newEndTime
    else:
        logger.info(f"Data for range from {query_params.get("starttime")} and {query_params.get("endtime")} is being processed")

        data = DataExt.get_data_real_time(query_params=query_params)
        l=DataExt.flatten_json(data=data)
        DataExt.parquet_file_path_real_time()

        newStartTime = prevEndTime
        newEndTime = prevEndTime + timedelta(minutes=5)
        query_params["starttime"]=newStartTime        
        query_params["endtime"] = newEndTime

    time.sleep(5)
