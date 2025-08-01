import requests
import pandas as pd
import os
import pytz
from p import query_params
from datetime import datetime

# from producer import send_to_kafka
START_YEAR = query_params.get("starttime") 
END_YEAR = query_params.get("endtime")
PATH = r"C:\Users\SohamKore\Desktop\Tasks\parquet_year_data"
URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"
if not os.path.exists(PATH):
    os.makedirs(PATH)


class DataExt:
    # fetching the data function 
    def get_data_real_time(query_params):
        response = requests.get(URL,params=query_params)
        if response.status_code==200:
            print("Successful")
        else:
            print(f"Error {response.status_code}")
        return response.json()
    # checks if messages are exceeded
    def check_total_count(data):
        total_count = data.get("metadata").get("count")
        if total_count==20000:
            return 1
        else:
            return 0

  # flattens json data by getting required params
    def flatten_json(data):
        l=[]
        # data = get_data(query_params=query_params)
        for d in data['features']:
            dic={
            "id":d.get("id"),
            "mag" : d.get('properties',[]).get('mag',[]),
            "place": d.get('properties',[]).get("place",[]),
            "time" : d.get('properties',[]).get("time",[]),
            "updated" : d.get('properties',[]).get("updated",[]), 
            "tz" : d.get('properties',[]).get("tz",[]),
            "url" :d.get('properties',[]).get("url",[]),
            "felt" : d.get('properties',[]).get("felt",[]),
            "alert" : d.get('properties',[]).get("alert",[]),
            "status" : d.get('properties',[]).get("status",[]),
            "tsunami" : d.get('properties',[]).get("tsunami",[]),
            "sig" : d.get('properties',[]).get("sig",[]),
            "code" :d.get('properties',[]).get("code",[]),
            "type" : d.get('properties',[]).get("type",[]),
            "title" :d.get('properties',[]).get("title",[]),
            "g_type":d.get("geometry",[]).get("type",[]),
            "latitude":d.get("geometry",[]).get("coordinates",[])[0],
            "longitude":d.get("geometry",[]).get("coordinates",[])[1],
            "dept":d.get("geometry",[]).get("coordinates",[])[2]
            }
            l.append(dic)
        return l
    # converts the list first to dataframe and then to parquet

    def to_parquet(data,filepath):
        print("in to_parquet function")
        df= pd.DataFrame(data)
        # send_to_kafka(df)
        df_par = df.to_parquet(filepath)
        return df_par
    
    # converts string to datetime function
    def string_to_datetime(date_str):
        return datetime.strptime(date_str, '%Y-%m-%d')
    
    # stores the parquet data in a specified file path
    def parquet_file_path():
        START_YEAR = query_params.get("starttime",[])
        END_YEAR = query_params.get("endtime",[])
        print("in parquet_file_path function")
        file_name = f"{START_YEAR} to {END_YEAR}.parquet"
        path_name = os.path.join(PATH,file_name)
        print(path_name)
        return DataExt.to_parquet(data=DataExt.flatten_json(),filepath=path_name)

    def parquet_file_path_real_time(sttime,entime):
        print("in parquet_file_path function")

        check_total_count = DataExt.check_total_count(data=DataExt.get_data_real_time(query_params=query_params))

        if check_total_count !=0:
            stime = str(query_params.get("starttime"))[0:10]
            etime = str(query_params.get("endtime"))[0:10]
            # checking if total messages is more than 20,000 in the check_total_count function
            count_of_data = DataExt.get_data_real_time(query_params=query_params).get("metadata",[]).get("count")
            prevStartTime = query_params.get("starttime")
            prevStartTime=pd.to_datetime(prevStartTime)
            prevEndTime = query_params.get("endtime")
            prevEndTime = pd.to_datetime(prevEndTime)
            year= prevEndTime.year
            print("total count over 20000, splitting the data in two parts")
            s = DataExt.string_to_datetime(stime).timestamp()
            e=  DataExt.string_to_datetime(etime).timestamp()
            print(s,e)
            midtime = s + (e-s)/2
            print(midtime)
            ts = datetime.fromtimestamp(midtime,tz=pytz.utc).strftime('%Y-%m-%d')
            print(ts)
            query_params["starttime"]=stime        
            query_params["endtime"] = str(ts)
            DataExt.parquet_file_path_real_time(sttime=stime,entime=str(ts))
            query_params["starttime"]=str(ts)    
            query_params["endtime"] = etime
            DataExt.parquet_file_path_real_time(sttime=str(ts),entime=etime)
        else:
            print(f"parameters sttime is{sttime}, and entime is {entime}")
            print(f"{type(sttime)},{type(entime)}")
            START_TIME = sttime.replace(":","_")
            END_TIME = entime.replace(":","_")

            print(START_TIME,END_TIME)
            file_name = f"{START_TIME[0:10]} to {END_TIME[0:10]}.parquet"
            path_name = os.path.join(PATH,file_name)
            print(path_name)
            return DataExt.to_parquet(data=DataExt.flatten_json(data=DataExt.get_data_real_time(query_params=query_params)),filepath=path_name)

