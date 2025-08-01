import calendar
import pandas as pd
from earthquake_module import DataExt
from datetime import timedelta 
from p import query_params

URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"
print(query_params)

def main():
    while(True):
        stime = str(query_params.get("starttime"))[0:10]
        etime = str(query_params.get("endtime"))[0:10]
    
        # check_total_count = DataExt.check_total_count(data=DataExt.get_data_real_time(query_params=query_params))
        count_of_data = DataExt.get_data_real_time(query_params=query_params).get("metadata",[]).get("count")
        prevStartTime = query_params.get("starttime")
        prevStartTime=pd.to_datetime(prevStartTime)
        prevEndTime = query_params.get("endtime")
        prevEndTime = pd.to_datetime(prevEndTime)
        year= prevEndTime.year
        print(f"Data for range from {query_params.get("starttime")} and {query_params.get("endtime")} is being processed")
        print(f"Total number of messages are {count_of_data}")
        data = DataExt.get_data_real_time(query_params=query_params)
        l=DataExt.flatten_json(data=data)
        DataExt.parquet_file_path_real_time(sttime=stime,entime=etime)
        newStartTime = prevEndTime
        if calendar.isleap(year=year):
            newEndTime = prevEndTime + timedelta(days=366)
        else:
            newEndTime = prevEndTime + timedelta(days=365)
        query_params["starttime"]=newStartTime        
        query_params["endtime"] = newEndTime
        # time.sleep(5)



if __name__ =='__main__':
    main()