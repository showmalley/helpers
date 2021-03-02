#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
This python class is a wrapper on connecting to, and performing operations with, snowflake databases using a VPN authentificator
"""

import pandas as pd
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL 
from sqlalchemy.dialects import registry
registry.register('snowflake', 'snowflake.sqlalchemy', 'dialect')

class client:
    
    def __init__(self, account, username, authenticator, warehouse, database, schema = None):
        
        # Attributes
        self.account = account
        self.username = username
        self.authenticator = authenticator
        self.warehouse = warehouse
        self.database = database
        self.schema = schema
        
        # Instantiate Engine
        self.engine = create_engine(URL(account = self.account,
                                        user = self.username,
                                        authenticator = self.authenticator,
                                        warehouse = self.warehouse,
                                        database = self.database,
                                        schema = self.schema))
        
    def read(self, query):
        
        """
        Read snowflake table into script
        
        Params:
            query : string containing snowflake query
        """
    
        with self.engine.connect() as con:
    
            df = pd.read_sql(query, con)
            
            print(f"--- Read Query From Snowflake")
            
        return df
        

    def create(self, data, table_name, index = False):
        
        """
        Create a snowflake table using a pandas dataframe
        
        Params:
            data : pandas dataframe
            table_name : name of new table to go into snowflake, given engine parameters
            index : binary indicator of inclusion of pandas index in write
        """
    
        with self.engine.connect() as con:
                
            data.to_sql(name=table_name, 
                        con=con, 
                        if_exists="replace", 
                        index=index)
            
            print(f"--- Completed {table_name} Create In Snowflake")

    def execute(self, query):
        
        """
        Given A Query, Execute anything with the instantiated engine
        
        Params:
            query : string containing snowflake query
            
        Examples:
            drop table : 'drop table t2'
            truncate : 'truncate table if exists t2'
            delete : 'delete from t2 where t2.k = 0.0'
            
        """
    
        with self.engine.connect() as con:
                
            con.execute(query)
            
            print(f"--- Completed Query In Snowflake")            
            
    def append(self, data, table_name, index = False):
        
        """
        Append a pandas dataframe to an existing table in snowflake
        
        Params:
            data : pandas dataframe
            table_name : name of snowflake table that will be appended, given engine parameters
            index : binary indicator of inclusion of pandas index in write
            
        """
    
        with self.engine.connect() as con:
            
            data.to_sql(name=table_name,
                        con=con,
                        if_exists="append",
                        index=index)
            
            print(f"--- Completed {table_name} Append In Snowflake")
            
    def close_conn(self):
        
        self.engine.dispose()
        
        print("Connection Closed & Disposed")
        
            
