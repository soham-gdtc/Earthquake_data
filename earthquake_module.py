import requests
import json
import pandas as pd
import os
from p import query_params
# from producer import send_to_kafka
START_YEAR = query_params.get("starttime") 
END_YEAR = query_params.get("endtime")
PATH = r"C:\Users\SohamKore\Desktop\Tasks\parquet_10min"
URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"
if not os.path.exists(PATH):
    os.makedirs(PATH)


class DataExt():

    def get_data(query_params):
        response = requests.get(URL,params=query_params)
        if response.status_code==200:
            print("Successful")
        else:
            print(f"Error {response.status_code}")
        return response.json()


    def get_data_real_time(query_params):
        response = requests.get(URL,params=query_params)
        if response.status_code==200:
            print("Successful")
        else:
            print(f"Error {response.status_code}")
        return response.json()


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

    # stores the parquet data in a specified file path
    def parquet_file_path():
        START_YEAR = query_params.get("starttime",[])
        END_YEAR = query_params.get("endtime",[])
        print("in parquet_file_path function")
        file_name = f"{START_YEAR} to {END_YEAR}.parquet"
        path_name = os.path.join(PATH,file_name)
        print(path_name)
        return DataExt.to_parquet(data=DataExt.flatten_json(),filepath=path_name)

    def parquet_file_path_real_time():
        print("in parquet_file_path function")
        START_TIME = str(query_params.get("starttime",[]))[0:19]
        END_TIME = str(query_params.get("endtime",[]))[0:19]
        START_TIME = START_TIME.replace(":","_")
        END_TIME = END_TIME.replace(":","_")

        print(START_TIME,END_TIME)

        # s_time=query_params.get("starttime",[])
        # e_time = query_params.get("endtime",[])
        # s_time_year =START_TIME[0:4]
        # s_time_month,e_time_month = START_TIME[5:7],END_TIME[5:7]   # month data
        # s_time_day, e_time_day = START_TIME[8:10],END_TIME[8:10] #day data
        # if  not os.path.exists(os.path.join(PATH,s_time_year,s_time_month)):  
        #     print("making new folder, required folder does not exist")
        #     os.makedirs(s_time_month)
        #     print("folder created")
        # else:
        #     print("folder exists here")
        #     file_name = f"{s_time_day} to {e_time_day}.parquet"
        #     path_name = os.path.join(PATH,s_time_year,s_time_month,file_name)
        #     return DataExt.to_parquet(data=DataExt.flatten_json(data=DataExt.get_data(query_params=query_params)),filepath=path_name
        
        # print(START_TIME,END_TIME)
        file_name = f"{START_TIME[0:10]} to {END_TIME[0:10]}.parquet"
        path_name = os.path.join(PATH,file_name)
        print(path_name)
        return DataExt.to_parquet(data=DataExt.flatten_json(data=DataExt.get_data(query_params=query_params)),filepath=path_name)

