#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
This module serves as an api wrapper to easily attain geo data from Google Maps

@author_company: Ameliorate, LLC
@author_name: Sean OMalley
@author_email: seanlomalley@protonmail.com
"""

import pandas as pd
import numpy as np
import googlemaps
import itertools

class GMaps:
    
    def __init__(self, api_key):
        
        # connect to Google Maps
        self.gmaps = googlemaps.Client(key = api_key)
        
        
    def reverse_geocode(self, df, lat_col, long_col):
        
        """
        Params:
            df: (dataframe object) pandas dataframe containing lat/long values
            lat_col: (string) pandas column name indicating longitude
            long_col: (string) pandas column name indicating latitude
            
        Returns:
            pandas DataFrame containing the best possible (if possible) address given the lat/long delivered
            
        """

        # initialize final_df
        geo_final = pd.DataFrame()
        skipped = 0
        
        # loop through all lat longs to get Google metadata
        for i in range(0, len(df)):
            
            try:
            
                # isolate lat long 
                lat = df.loc[i,lat_col]
                long = df.loc[i,long_col]
        
                # reverse geocode to dataframe
                reverse_geo = self.gmaps.reverse_geocode((lat, long))
                temp = pd.DataFrame(reverse_geo).reset_index(drop = False).rename(columns = {"index":"address_rank"})
                temp[lat_col] = lat
                temp[long_col] = long
                geo_final = geo_final.append(temp).reset_index(drop=True)
                
            except:
                
                skipped += 1
                next
                
        # create compound and global (and drop photo and icon bc useless)
        geo_final["compound_code"] = [geo_final.loc[x,"plus_code"]["compound_code"] if pd.isnull(geo_final.loc[x,"plus_code"]) == False else np.nan for x in range(0, len(geo_final))]
        geo_final["global_code"] = [geo_final.loc[x,"plus_code"]["global_code"] if pd.isnull(geo_final.loc[x,"plus_code"]) == False else np.nan for x in range(0, len(geo_final))]
        geo_final = geo_final.drop(columns = ["plus_code"])
        
        # create lat long from geometry
        geo_final["gmaps_lat"] = [x["location"]["lat"] for x in geo_final["geometry"]]
        geo_final["gmaps_long"] = [x["location"]["lng"] for x in geo_final["geometry"]]
        geo_final = geo_final.drop(columns = ["geometry","address_components"])
        
        # create binary for place type
        a = [x for x in geo_final["types"]]
        a_set = list(set(list(itertools.chain.from_iterable(a))))
        
        # save binaries as new columns
        for col in a_set:
            geo_final.loc[:,col] = [1 if col in geo_final.loc[x,"types"] else 0 for x in range(0, len(geo_final))]
        
        # drop types now that binary
        geo_final = geo_final.drop(columns = ["types"])
        
        return geo_final
    
    
    def places_nearby(self, df, lat_col, long_col):
        
        """
        Params:
            df: (dataframe object) pandas dataframe containing lat/long values
            lat_col: (string) pandas column name indicating longitude
            long_col: (string) pandas column name indicating latitude
            
        Returns:
            pandas DataFrame containing details about places near the lat/long, their residential/commercial status and subsequent data points
            
        """
        
        ### Step 1: Make API Call and Build Result List

        places_results_list = []
        skipped = 0 
        
        # loop places API call
        for j in range(0, len(df)):
               
            try:
                    
                # isolate lat/long
                u_lat = df.loc[j,'centroid_latitude']
                u_long = df.loc[j,'centroid_longitude']
            
                # hit google places API for locations within 50 meters
                google_places_nearby = self.gmaps.places_nearby(location = (f"{u_lat},{u_long}"), 
                                                           radius = 25, # meters from data point
                                                           open_now = False)
                
                # isolate places result 
                gpn_result = google_places_nearby["results"]
                
                # append to list
                places_results_list.append({"lat":u_lat, "long": u_long, "result":gpn_result})
                
            except:
                skipped += 1
                next
                
        ### Step 2: Manipulate Results and Optimize for DS

        final = pd.DataFrame()
        
        for i in range(0, len(places_results_list)):
            
            # build dataframe for each record that comes through
            result_temp = places_results_list[i]["result"]
            result_temp = pd.DataFrame(result_temp)
            result_temp["centroid_lat"] = places_results_list[i]["lat"]
            result_temp["centroid_long"] = places_results_list[i]["long"]
            result_temp = result_temp.T
            
            # liberal merge on index 
            if i == 0:
                final = result_temp
            else:
                final = final.merge(result_temp, left_index = True, right_index = True, how = "outer")
        
        # transpose back
        final = final.T.reset_index(drop = True)
        
        # create lat long from geometry
        final["places_lat"] = [x["location"]["lat"] for x in final["geometry"]]
        final["places_long"] = [x["location"]["lng"] for x in final["geometry"]]
        final = final.drop(columns = ["geometry"])
        
        # create compound and global (and drop photo and icon bc useless)
        final["compound_code"] = [final.loc[x,"plus_code"]["compound_code"] if pd.isnull(final.loc[x,"plus_code"]) == False else np.nan for x in range(0, len(final))]
        final["global_code"] = [final.loc[x,"plus_code"]["global_code"] if pd.isnull(final.loc[x,"plus_code"]) == False else np.nan for x in range(0, len(final))]
        final = final.drop(columns = ["plus_code","photos","icon","opening_hours"])
        
        # create binary for place type
        a = [x for x in final["types"]]
        a_set = list(set(list(itertools.chain.from_iterable(a))))
        
        # save binaries as new columns
        for col in a_set:
            final.loc[:,col] = [1 if col in final.loc[x,"types"] else 0 for x in range(0, len(final))]
        
        # drop types now that binary
        final = final.drop(columns = ["types"])
        
        return final
    
def geo_encode(df, lat, long, precision = 23):
    
    """
    The intent of this function is to take a pyspark dataframe with the lat/long column names specified and return a geohash column included
    
    Params:
        df: (pyspark dataframe) pyspark dataframe containing lat/long columns
        lat: (str) indicator of column name of latitude values
        long: (str) indicator of column name of longitude values
        precision: (int) length of the geohash, default set to 23 character geohash because this equals 6 decimals of precision 
    Returns:
        pyspark dataframe with additional column
    """

    # convert data types
    df = df.withColumn(lat, df[lat].cast(DecimalType(10,6)).alias(lat))
    df = df.withColumn(long, df[long].cast(DecimalType(10,6)).alias(long))

    # create geohash function
    ps_geohash_encode = udf(lambda x,y: "unknown" if pd.isnull(x) or pd.isnull(y) else geohash.encode(x,y,precision=precision))

    # apply it to dataframe
    df = df.withColumn("GeoHash",ps_geohash_encode(lat,long))
    
    return df

def geo_decode(df, code):
    
    """
    The intent of this function is to take a pyspark dataframe with the lat/long column names specified and return the dataframe lat/long columns included
    
    Params:
        df: (pyspark dataframe) pyspark dataframe containing geohash columns
        code: (str) geocode column name to be decoded utilizing (python-geocode pip package)
    Dependencies:
        with_column_index: (udf) adds column index to pyspark dataframe
    Returns:
        pyspark dataframe with additional columns
    """
    
    # decode geohash function
    schema = StructType([StructField("Latitude", FloatType(), False), StructField("Longitude", FloatType(), False)])
    geohash_decode = udf(lambda x: (None,None) if x == "unknown" else geohash.decode(x), schema)

    # apply results of two columns to one
    schema = StructType([StructField("Latitude", FloatType(), False), StructField("Longitude", FloatType(), False)])

    # run decode to two columns
    temp = df.select(geohash_decode(code).alias("ReverseGeoHash"))

    # create column indexes
    temp_ci = with_column_index(temp.select("ReverseGeoHash.*"))
    df_ci = with_column_index(df)

    # join original to original dataframe
    df = df_ci.join(temp_ci, df_ci.ColumnIndex == temp_ci.ColumnIndex, 'inner').drop("ColumnIndex")
    
    return df