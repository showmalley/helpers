# -*- coding: utf-8 -*-
"""
This module is intended crawl an AWS database, re-partition by memory, split out event from reference data and .

@author_company: Ameliorate, LLC
@author_name: Sean OMalley
@author_email: seanlomalley@protonmail.com
"""

# import packages
from ameliorate_repo import aws_etl, iab_code_scraper
import pandas as pd
import numpy as np
import time
import random
import warnings
warnings.filterwarnings("ignore")

class RelationalCrawler:
    
    def __init__(self, secret, access_id, bucket_name):
        
        # Instantiate AWS helper class
        self.as3 = aws_etl.S3(secret = secret, 
                         access_id = access_id, 
                         bucket_name = bucket_name)
           
    
    def create_device_master(self, df):

        # isolate apps by names
        df = df[['DeviceID', 'AppName',"IP"]].drop_duplicates()
        
        # isolate top hit IP
        top_ip = df.groupby(["DeviceID","IP"])["IP"].count().sort_values(ascending = False)
        top_ip = pd.DataFrame(top_ip).rename(columns={'IP':'COUNT'}).reset_index(drop = False).drop_duplicates(["DeviceID"], keep = "first").drop(columns = ["COUNT"])
        df = df.drop(columns = ["IP"]).merge(top_ip, on = ["DeviceID"], how = "left")
        
        # create android/apple binary
        df["iOS"] = np.where(df["AppName"].str.upper().str.contains(' IOS'), 1, 0)
        df["Android"] = np.where(df["AppName"].str.upper().str.contains(' ANDROID'), 1, 0)
    
        # group to device id
        df = df.drop(columns = ["iOS"]).merge(df.groupby(["DeviceID"])["iOS"].sum().reset_index(), on = "DeviceID", how = "left")
        df = df.drop(columns = ["Android"]).merge(df.groupby(["DeviceID"])["Android"].sum().reset_index(), on = "DeviceID", how = "left")
    
        # make binary  
        df["iOS"] = np.where(df["iOS"] > 0, 1, 0)
        df["Android"] = np.where(df["Android"] > 0, 1, 0)
    
        # isolate devices
        df = df[['DeviceID','iOS','Android']].drop_duplicates(["DeviceID"])
    
        return df

    ### Create AppMaster
    
    def create_app_master(self, df, iab_df):
        
        # isolate apps by names
        df = df[['AppCategory', 'AppName']].drop_duplicates(['AppName'])
    
        # remove android/ios within app name
        df["AppName"] = df["AppName"].str.upper().replace(" IOS","",regex=True).replace(" ANDROID","",regex=True).str.strip()
    
        # drop duplicates again on app name
        df = df.drop_duplicates(['AppName'])
    
        # alter string list to py list
        df["AppCategory"] = [x.upper().replace("|",";").split(";") for x in df["AppCategory"]] 
    
        # creqte unqiue list of categories
        cat_list = [x for x in df["AppCategory"]]
        cat_list = list(set([val for sublist in cat_list for val in sublist]))
    
        # iterate categories to see if category is contained, binary
        for cat in cat_list:
            temp_list = []
            for row in df["AppCategory"]:
                if cat in row:
                    temp_list.append(1)
                else:
                    temp_list.append(0)
            df[cat] = temp_list
    
        # drop category column
        df = df.drop(columns = ["AppCategory"])
    
        # one last dup drop
        df = df.drop_duplicates(['AppName'])
        
        # make unique identifier based on name
        df["AppID"] = [str(hash(x))[1:13] for x in df["AppName"]]
            
        # bring to front
        df = df.set_index(["AppID"]).reset_index(drop = False)
        
        # add IAB values where we can
        df = df.T.rename(index=dict(iab_df.values)).T
        
        return df
    
    #### Create EventMaster
    
    def create_event_master(self, df, app_df):
        
        # isolate needed columns
        df = df[["Timestamp","DeviceID","AppName","Latitude","Longitude","InternetType"]]
    
        # remove android/ios within app name
        df.loc[:,"AppName"] = df["AppName"].str.upper().replace(" IOS","",regex=True).replace(" ANDROID","",regex=True).str.strip()
    
        # make internet type binary
        df.loc[:,"Cellular"] = np.where(df["InternetType"] == "CELLULAR", 1, 0)
        df = df.drop(columns = ["InternetType"]).drop_duplicates()
        
        # use app uid rathe than AppName
        df = df.merge(app_df[["AppID","AppName"]], on = ["AppName"], how = "left").drop(columns = ["AppName"])
        
        # re-order
        df = df[['Timestamp', 'DeviceID', 'AppID', 'Cellular', 'Latitude', 'Longitude']]
        
        return df
    
    #### Create IPMaster
    
    def create_ip_master(self, df):
        
        # we only need to drop duplicates on IP to reduce size
        df = df[["IP","ISP"]].drop_duplicates(["IP"])
        
        return df
    
    #### Parition to Relational
    
    def partition(self, df, iab_df):
        
        # place each into respective dataset
        dm = self.create_device_master(df)
        am = self.create_app_master(df, iab_df)
        im = self.create_ip_master(df)
        em = self.create_event_master(df, am)
        
        return dm, am, im, em
    
    #### Crawl, Relationalize and Write
    
    def run(self, meta_list, event_path, resource_path, day_part = None, sample = None, flag = 0, byte_thresh = 1e+9):
        
        # time testing
        t0 = time.time()
        
        # scrape iab codes
        iab_df = iab_code_scraper.refresh_codes()
        self.as3.write_parquet(df = iab_df, object_name = "IAB_DM", path = "relational/resources/")
    
        # create sampling option
        if sample == None:
            meta_list_run = meta_list
        else:
            meta_list_run = meta_list[:sample]
            
        for i in range(0,len(meta_list_run)):
            
            try:
    
                # ingest it
                temp = self.as3.read_parquet(path = meta_list[i])
                
                # clean it or not to clean it, that is the question?
                #temp = temp.dropna()
        
                # partition it
                dm, am, im, em = self.partition(temp, iab_df)
                
                # for every 8th run, reset event_mart for partition
                if flag == 1:
                    
                    event_mart = em.copy()
                    print("----- Reset Events")
                    
                # flag that switches when events write to aws
                flag = 0
        
                # instantiate marts on first run
                if i == 0:
        
                    device_mart = dm.copy()
                    app_mart = am.copy()
                    ip_mart = im.copy()
                    event_mart = em.copy()
        
                else:
        
                    # controlled output, simply concat
                    device_mart = pd.concat([device_mart,dm], sort = False).drop_duplicates(["DeviceID"])
        
                    # transpose merge on index to ensure data quality of union
                    app_mart = app_mart.T.merge(am.T, left_index = True, right_index = True, how = "outer").T.drop_duplicates(["AppName"]).reset_index(drop = True)
        
                    # transpose merge on index to ensure data quality of union
                    ip_mart = ip_mart.T.merge(im.T, left_index = True, right_index = True, how = "outer").T.drop_duplicates(["IP"]).reset_index(drop = True)
        
                    # keep appending, else repartition by memory
                    if event_mart.memory_usage().sum().sum() <= byte_thresh:
        
                        # transpose merge on index to ensure data quality of union
                        event_mart = event_mart.T.merge(em.T, left_index = True, right_index = True, how = "outer").T.drop_duplicates().reset_index(drop = True)
        
                    else:
                        
                        print(f"Partition created at {event_mart.memory_usage().sum().sum()/2e+8} MB of memory")
        
                        # easy dates
                        #start = datetime.datetime.fromtimestamp(int(event_mart.Timestamp.min()) / 1e3)
                        #start = pd.to_datetime(start).strftime("%Y%m%d")
                        
                        #end = datetime.datetime.fromtimestamp(int(event_mart.Timestamp.max()) / 1e3)
                        #end = pd.to_datetime(end).strftime("%Y%m%d")
                        
                        # write events iteration and full DMs to parquet
                        self.as3.write_parquet(df = event_mart, object_name = f"EVENT_DM_{random.getrandbits(64)}",  path = event_path)
                        self.as3.write_parquet(df = device_mart, object_name = "DEVICE_DM", path = resource_path)
                        self.as3.write_parquet(df = app_mart, object_name = "APP_DM", path = resource_path)
                        self.as3.write_parquet(df = ip_mart, object_name = "IP_DM", path = resource_path)
                        
                        # flag that switches when events write to aws
                        flag = 1
                        
                        #print(f"Wrote EVENT_DM_{start}-{end} to {event_path}")'
                    
            except:
                
                print(f"----- Skipped {meta_list[i]}")
                next
                    
        # write each final resource to parquet
        self.as3.write_parquet(df = device_mart, object_name = "DEVICE_DM", path = resource_path)
        self.as3.write_parquet(df = app_mart, object_name = "APP_DM", path = resource_path)
        self.as3.write_parquet(df = ip_mart, object_name = "IP_DM", path = resource_path)
        self.as3.write_parquet(df = event_mart, object_name = f"EVENT_DM_{random.getrandbits(64)}", path = event_path)
            
        t1 = time.time()
        total = t1-t0
        
        print(f"Relational-ization took {round(total/60,2)} minutes.")