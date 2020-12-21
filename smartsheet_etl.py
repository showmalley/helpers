#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
This module is intended to serve as a wrapper for the smartsheet API in order to make for ease of use.

@author_company: Ameliorate, LLC
@author_name: Sean OMalley
@author_email: seanlomalley@protonmail.com

"""

import smartsheet
import pandas as pd
from simple_smartsheet.models import Sheet, Column, Row, ColumnType

##### Smartsheet API Helper Class

class ss_client:
    
    def __init__(self, token):
    
        """
        instantiate class and initialize smartsheet connection given token
        """
        self.smart = smartsheet.Smartsheet(token)

    def smartsheet_get(self, sheet_id):
        
        """
        retrieves smartsheet object via api and retuns sheet object
        """
        
        sheet = self.smart.Sheets.get_sheet(sheet_id = sheet_id)
        
        return sheet
    
    def simple_sheet_to_dataframe(sheet):
        
        """
        turns sheet object to pandas dataframe for ease of use
        """
        
        col_names = [col.title for col in sheet.columns]
        
        rows = []
        for row in sheet.rows:
            cells = []
            for cell in row.cells:
                cells.append(cell.value)
            rows.append(cells)
            
        data_frame = pd.DataFrame(rows, columns=col_names)
        
        return data_frame
    
    def tl_ss(self, sheet_name, df, key):
        
        
        """
        process: (back end of ss is quirky, so these steps are necessary)
        
            1. delete sheet name if exists
            2. create new sheet skeleton, specified by list of columns
            3. iteratively add new rows using dataframe data
            
        params:
            
            sheet_name: smartsheet name for column
            
            df: pandas dataframe which we are placing into smartsheets
            
            key: establish key column
            
        returns:
            
            writes dataframe to smartsheet under specified sheet name if completed
                
                
        """
        
        # isolate df columns
        cols = list(df.dtypes.index)
        skeleton = [Column(title=x, type=ColumnType.TEXT_NUMBER) if x != key else Column(primary=True, title=x, type=ColumnType.TEXT_NUMBER) for x in cols]
        
        # get sheets associated with token
        sheets = self.smart.sheets.list()
        
        # delete the test sheet if already exists
        for sheet in sheets:
            if sheet.name == sheet_name:
                smartsheet.sheets.delete(id=sheet.id)
                
        # create a new sheet skeleton
        new_sheet_skeleton = Sheet(name=sheet_name, columns=skeleton)
        
        # add the blank sheet via API
        result = smartsheet.sheets.create(new_sheet_skeleton)
        sheet = result.obj
        print(f"ID of the created sheet is {sheet.id!r}")
        
        # isolate sheet
        sheet = self.smart.sheets.get(sheet_name)
        
        # create new row list
        new_rows = []
        for i in range(0,len(df)):
            
            new_rows.append(Row(to_top=True,
                                cells=sheet.make_cells({x: df.loc[i,x] for x in cols})))
        
        # write to ss
        smartsheet.sheets.add_rows(sheet.id, new_rows)
        
        # sort
        sheet = smartsheet.sheets.sort_rows(
            sheet, [{"column_title": key, "descending": False}]
        )
        