#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@author: somalley@centura.org
"""

import sqlalchemy as db
import pandas as pd
import pymssql
    
class client:
    
    def __init__(self, username, password, host, port, database = ''):
        
        self.engine = db.create_engine('mssql+pymssql://' + username + ':' + password + '@'+ host + ':' + port + '/' + database + '?charset=utf8')
        self.connection = self.engine.connect()
        self.database = database
        self.host = host
        self.port = port
        
    def truncate(self, name, schema):
        self.connection.execute( f"""TRUNCATE TABLE {self.database}.{schema}.{name}""" )
 
    def from_sql(self, query):
        self.result = pd.read_sql(query, self.connection)
        
    def to_sql(self, data, name, schema, if_exists, index = False, dtypes = None):
        data.to_sql(name = name, con = self.connection, schema =schema, if_exists = if_exists, index = index, dtype = dtypes)
            
    def close_conn(self):
        self.connection.close()
        