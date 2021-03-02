#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
This module is intended to serve as a wrapper for AWS functionalities in order to make for ease of use.
"""

# import packages
import boto3
from io import StringIO, BytesIO
import pandas as pd
import json
import botocore
import pyarrow.parquet as pq
import io

# class to ease operation of s3
class S3:
    
    def __init__(self, secret, access_id, bucket_name = None, region = None):
        
        """
        Params:
            secret: (string) aws_secret_access_key used to access AWS account
            access_id: (string) aws_access_key_id used to access AWS account
            bucket_name: (string) (string) name of bucket of operation, option to specify at instantiation or within method
            region: (string) region_name of aws datacenter used for operation
        """
        
        # instantiate bucket name, not required
        self.bucket_name = bucket_name
        
        # flexible region options (default to us-east-2 if not specified)
        if region == None:
            self.region = "us-east-2"
        else:
            self.region = region
        
        # Connect to s3 Client
        self.resource = boto3.resource('s3',
                                     region_name =  self.region,
                                     aws_access_key_id = access_id,
                                     aws_secret_access_key = secret)
        
        # Connect to s3 Client
        self.client = boto3.client('s3',
                                   region_name =  self.region,
                                   aws_access_key_id = access_id,
                                   aws_secret_access_key = secret)
    
    def multi_file_filter(self, prefix, bucket_name = None, aslist = None):
        
        """
        Params:
            prefix: (string) file location prefix where all underlying files will be placed into s3 list object
            bucket_name: (string) name of bucket of operation, option to specify at instantiation or within method
            
        Returns:
            An S3 collection of objects given the prefix and button specifification, or casts as a list of paths if specified
        """
        
        # flexible bucket options (on instantiation or within method)
        if self.bucket_name == None:
            print("Please Choose Bucket Name to Continue")
            
        elif bucket_name == None:
            
            bucket_name = self.bucket_name
            
        # isolate bucket
        play_bucket = self.resource.Bucket(bucket_name)
        
        # grab chunk of objects given prefix
        multi_objects = play_bucket.objects.filter(Prefix = prefix)
        
        if aslist == True:
            multi_objects = [x.key for x in multi_objects.all()]
        
        return multi_objects
    
    def multi_read(self, multi_objects, bucket_name = None):
        
        """
        Params:
            multi_objects: (s3 object list) agglomeration of s3 objects, or list of filenames
            bucket_name: (string) name of bucket of operation, option to specify at instantiation or within method
            
        Returns:
            If successful, writes dataframe with specified file location and name to s3 as csv
        """
        
        # flexible bucket options (on instantiation or within method)
        if self.bucket_name == None:
            print("Please Choose Bucket Name to Continue")
            
        elif bucket_name == None:
            
            bucket_name = self.bucket_name
        
        # build dataset concatenating temp objects
        df = pd.DataFrame()
        
        if type(multi_objects) != list:
            
            # based on s3 object list
            for thing in multi_objects.all():
                

                
                if thing.key.endswith(".csv"): # need to change this, not getting picked up
                    df = self.read_csv(path = thing.key, bucket_name = bucket_name)
                    print("---")
                    print(f"Read {thing.key}")
                    df = df.append(df)
                    print(f"Appended {thing.key}")                
                elif (thing.key.endswith(".parquet")) | (thing.key.endswith(".pq"))  | (thing.key.endswith(".parquet.snappy")):
                    df = self.read_parquet(path = thing.key, bucket_name = bucket_name)
                    print("---")
                    print(f"Read {thing.key}")
                    df = df.append(df)
                    print(f"Appended {thing.key}")
                
        elif type(multi_objects) == list:
            
            # based on list of object names
            for thing in multi_objects.all():
                
                if str(thing).endswith(".csv"):
                    df = self.read_csv(path = thing, bucket_name = bucket_name)
                elif (str(thing).endswith(".parquet")) | (str(thing).endswith(".pq"))  | (str(thing).endswith(".parquet.snappy")):
                    df = self.read_parquet(path = thing, bucket_name = bucket_name)
                    
                df = df.append(df)
            
        return df
                
    def write_csv(self, df, object_name, path, bucket_name = None, feedback = None):
        
        """
        Params:
            df: (dataframe object) pandas dataframe intended to be written to an s3 location
            object_name: (string) name of the dataframe within the s3, will write as csv, so no need to include .csv
            dest_filepath: (string) destination filepath within s3
            bucket_name: (string) name of bucket of operation, option to specify at instantiation or within method
            feedback: (boolean) binary inicator of print feedback, defaulted to True
            
        Returns:
            If successful, writes dataframe with specified file location and name to s3 as csv
        """
        
        # flexible bucket options (on instantiation or within method)
        if self.bucket_name == None:
            print("Please Choose Bucket Name to Continue")
            
        elif bucket_name == None:
            
            bucket_name = self.bucket_name
            
            # create csv buffer and write pandas df to location
            csv_buffer = StringIO()
            df.to_csv(csv_buffer, index = False)
            self.resource.Object(bucket_name, f"{path}{str(object_name).split('.')[0]}.csv").put(Body=csv_buffer.getvalue())
            
            # create optional feedback, default to yes
            if feedback != False:
                print(f"Successfuly Wrote {str(object_name)} within bucket {bucket_name} to {path} ")
        
    def read_csv(self, path, bucket_name = None):
        
        """
        Params:
            path: (string) destination filepath and entire filename specified within s3 bucket
            bucket_name: (string) name of bucket of operation, option to specify at instantiation or within method
            
        Returns:
            If successful, reads data from s3 and returns pandas dataframe into memory
        """
        
        # flexible bucket options (on instantiation or within method)
        if bucket_name == None:
            bucket_name = self.bucket_name
        if self.bucket_name == None:
            print("Please Choose Bucket Name to Continue")
            
        # ingest object from S3
        obj = self.resource.Object(bucket_name, path)
        body = obj.get()['Body'].read()

        # byte to dataframe
        s = str(body,'utf-8')
        data = StringIO(s) 
        df = pd.read_csv(data)
        
        return df
    
    def write_dict(self, d, object_name, path, bucket_name = None, feedback = None):
        
        """
        Params:
            d: (dictionary object) dictionary to be written to an s3 location
            object_name: (string) name of the dataset within the s3, will write as csv, so no need to include .csv
            path: (string) destination filepath within s3
            bucket_name: (string) name of bucket of operation, option to specify at instantiation or within method
            feedback: (boolean) binary inicator of print feedback, defaulted to True
            
        Returns:
            If successful, writes dictionary object with specified file location and name to s3 as .dict
        """
        
        # flexible bucket options (on instantiation or within method)
        if self.bucket_name == None:
            print("Please Choose Bucket Name to Continue")
            
        elif bucket_name == None:
            
            bucket_name = self.bucket_name
            
            # create csv buffer and write pandas df to location
            self.resource.Object(bucket_name, f"{path}{str(object_name).split('.')[0]}.dict").put(Body=json.dumps(d))
            
            # create optional feedback, default to yes
            if feedback != False:
                print(f"Successfuly Wrote {str(object_name)} within bucket {bucket_name} to {path} ")
    
    def read_dict(self, path, bucket_name = None):
        
        """
        Params:
            path: (string) destination filepath and entire filename specified within s3 bucket
            bucket_name: (string) name of bucket of operation, option to specify at instantiation or within method
            
        Returns:
            If successful, reads data from s3 and returns dictionary in memory
        """
        
        # flexible bucket options (on instantiation or within method)
        if bucket_name == None:
            bucket_name = self.bucket_name
        if self.bucket_name == None:
            print("Please Choose Bucket Name to Continue")
            
        # ingest object from S3
        obj = self.resource.Object(bucket_name, path)
        body = obj.get()['Body'].read().decode('utf-8')

        # byte to dict
        d = json.loads(body)
        
        return d

    def write_parquet(self, df, object_name, path, bucket_name = None, feedback = None):
        
        """
        Params:
            df: (dataframe object) pandas dataframe intended to be written to an s3 location
            object_name: (string) name of the dataframe within the s3, will write as csv, so no need to include .csv
            dest_filepath: (string) destination filepath within s3
            bucket_name: (string) name of bucket of operation, option to specify at instantiation or within method
            feedback: (boolean) binary inicator of print feedback, defaulted to True
            
        Returns:
            If successful, writes dataframe with specified file location and name to s3 as csv
        """
        
        # flexible bucket options (on instantiation or within method)
        if self.bucket_name == None:
            print("Please Choose Bucket Name to Continue")
            
        elif bucket_name == None:
            
            bucket_name = self.bucket_name

            # upload to s3 
            out_buffer = BytesIO()
            df.to_parquet(out_buffer, index=False)
            self.resource.Object(bucket_name, f"{path}{str(object_name).split('.')[0]}.parquet").put(Body=out_buffer.getvalue())
            
            # create optional feedback, default to yes
            if feedback != False:
                print(f"Successfuly Wrote {str(object_name)} within bucket {bucket_name} to {path} ")
            
    def read_parquet(self, path, bucket_name = None):
        
        """
        Params:
            path: (string) destination filepath and entire filename specified within s3 bucket
            bucket_name: (string) name of bucket of operation, option to specify at instantiation or within method
            
        Returns:
            If successful, reads data from s3 and returns pandas dataframe into memory
        """
        
        # flexible bucket options (on instantiation or within method)
        if bucket_name == None:
            bucket_name = self.bucket_name
        if self.bucket_name == None:
            print("Please Choose Bucket Name to Continue")
            
        # ingest object from S3
        buffer = io.BytesIO()
        obj = self.resource.Object(bucket_name, path)
        obj.download_fileobj(buffer)
        table = pq.read_table(buffer)
        df = table.to_pandas()
        
        return df
    
    def parse_path(self, s3_path):
        
        """
        Params:
            path: (string) destination filepath and entire filename specified within s3 bucket
            
        Returns:
            Splits the string to return bucket and path names
        """
        # ensure dtype
        path = str(s3_path)
                
        # isolate path file structure
        if s3_path[:5] == "s3://":
            path = s3_path[5:]
             
        # split bucket and path
        bucket, obj = path.split("/",1)
        
        return bucket, obj
    
    def get_list(self, s3_path):
        
        """
        Params:
            path: (string) destination filepath and entire filename specified within s3 bucket
        Returns:
            python list given filepath of a txt file
        """
        
        # run parser
        bucket, obj_path = self.parse_path(s3_path)
        
        # ensure text files
        if obj_path[-4:] != ".txt":
            raise ValueError("Must be .txt file.")
            
        # ingest object from S3
        obj = self.resource.Object(bucket, obj_path)
        body = obj.get()['Body'].read()
        
        # byte to text to list
        s = str(body,'utf-8')
        l = s.splitlines()
        
        return l
    
    def create_file(self, file_path, bucket_name = None):
        
        # flexible bucket options (on instantiation or within method)
        if bucket_name == None:
            bucket_name = self.bucket_name
        if self.bucket_name == None:
            print("Please Choose Bucket Name to Continue")
        
        self.client.put_object(Bucket=bucket_name, Key=file_path)
        
        print(f"Created {file_path}")
        
    def obj_check(self, s3_path):
        
        """
        Params:
            path: (string) destination filepath and entire filename specified within s3 bucket
        Returns:
            python list given filepath of a txt file
        """
        
        # run parser
        bucket, obj_path = self.parse_path(s3_path)
        
        try:
            self.resource.Object(bucket, obj_path).load()
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == 404:
                return False
            else:
                raise Exception("Client Error Without 404")
        else:
            return True
        
    def crawl_size(self, prefix):
        
        """
        Params:
            prefix: (string) file location prefix where all underlying files will be placed into s3 list object
        Returns:
            pandas dataframe with medadata levels and size
            python list containing all filepaths given bucket and prefix
        """

        # isolate file list
        files = self.multi_file_filter(prefix = prefix)

        # create list of metadata with sizes
        meta_list = []
        for obj in files.all():
            meta_list.append(obj.key.split('/') + [obj.size])

        try:
            
            # create adaptable list for column names
            name_list = []
            for i in range(0,len(meta_list[0])-1):
                name_list.append(f"Level {str(i)}")
            name_list = name_list + ["Size"]
    
            # create dataframe from lists
            meta_df = pd.DataFrame(meta_list, columns = name_list)
            meta_df = meta_df[meta_df.Size > 0].dropna()
    
        except:
            
            # create adaptable list for column names
            name_list = []
            for i in range(0,len(meta_list[0])):
                name_list.append(f"Level {str(i)}")
            name_list = name_list + ["Size"]
    
            # create dataframe from lists
            meta_df = pd.DataFrame(meta_list, columns = name_list)
            meta_df = meta_df[meta_df.Size > 0].dropna()
            
        # zip up meta list to string
        meta_list = ['/'.join(x[:-1]) for x in meta_list]

        return meta_df, meta_list
