#!/usr/bin/env python
# coding: utf-8

# tableau python libraries to be installed
# 
# 1) tableauhyperapi
# 
# 2) tableauserverclient
# 
# pip install tableauhyperapi
# pip install tableauserverclient 
# 
# https://help.tableau.com/current/api/hyper_api/en-us/docs/hyper_api_installing.html
# 
# https://tableau.github.io/server-client-python/docs/
# 

# In[4]:


import tableauserverclient as tsc
from tableauhyperapi import Connection,HyperProcess,SqlType,TableDefinition,escape_string_literal,escape_name,NOT_NULLABLE,Inserter,CreateMode,TableName,Telemetry
from datetime import datetime
import pandas as pd
import os


# In[3]:
inDir = "" #input directory where input data is made available. In this example bitcoin_price_hist.csv
outDir = "" # output directory where hyper file will be genereated. 

os.chdir(outDir)

def hyper_auto(df,csv_file_name):#takes two arguments, 1) a dataframe which you want to convert to hyper 2) csv_file_name for driving hyper data set name.
    csv_file_patt = csv_file_name.split(".")[0] #removig .csv extension so that hyper file name can be drived.
    hyper_file = csv_file_patt + ".hyper" #hyper file name
    with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        print("hyper process has started")
        
        with Connection(hyper.endpoint, hyper_file, CreateMode.CREATE_AND_REPLACE) as connection:
            print("The connection to the .hyper file is open.")
            connection.catalog.create_schema('Extract')
            execution_table = TableDefinition(TableName('Extract','Extract'),[
                #sqltype date also available but using text as datatype for example, as datatypes can be changed in tableau while creating dashboards as well.
                #if you want to use date here, then it wont work with pandas data frame.Check all the available sql types in hyperextractapi documentation
                #below table definition has to be defined for any new hyper extract just like a new create statement is issued for creating any new table in database.
                #there is pandleau library also available for generating hyper extracts but the library can not be installed in all client enviornments due to security issues etc.
             TableDefinition.Column('Date',SqlType.text()),
             TableDefinition.Column('Open',SqlType.double()),
             TableDefinition.Column('High',SqlType.double()),
             TableDefinition.Column('Low',SqlType.double()),
             TableDefinition.Column('Close',SqlType.double()),
             TableDefinition.Column('Volume',SqlType.text()),
             TableDefinition.Column('Market Cap',SqlType.text())
            ])
            print("The table is defined")
            connection.catalog.create_table(execution_table)
            with Inserter(connection,execution_table) as inserter:
                for row in df.values.tolist():
                    inserter.add_row(row)
                inserter.execute()
            print("The data was added to the table.")
        print("The connection to the Hyper extract file is closed.")
    print("The hyper process has shut down.")
    return(hyper_file)

def publish_files(hyper_file,write_mode,project):
    tab_auth = tsc.TableauAuth('userid','password','siteid') #site id can be empty incasse of on premise tableau
    server = tsc.Server('https://tableau server address') # tableau server address should be changed
    server.add_http_options({'verify':'tls or ssl certificate path here'}) # this may not be required if its able to connect without certificate.
    with server.auth.sign_in(tab_auth):
        # first find out project id in which data source will be published.
        project_dtls = [proj for proj in tsc.Pager(server.projects) if proj.name == project]
        project_id = project_dtls[0].id
        # use project id to create data source
        new_datasource = tsc.DataSourceItem(project_id)
        # get list of already available data sources
        all_datasources,pagination_item = server.datasources.get()
        available_datasources = [datasource.name for datasource in all_datasources]
        # get write mode on basis of whether data source is available or not.
        # for first time data source write mode will be 'CreateNew'
        # once data source is available write mode could be 'Overwrite' or 'Append'
        datasource_name = hyper_file.split(".")[0]
        if datasource_name in available_datasources:
            write_mode_datasource = write_mode #this could be 'Overwrite' or 'Append'
        else:
            write_mode_datasource = 'CreateNew'
        
         #publish data source to server now
        server.datasources.publish(new_datasource,hyper_file,write_mode)
        # write mode will be either 'Overwrite' or 'CreateNew'

def main():
    df_bitcoin = pd.read_csv(inDir + "bitcoin_price_hist.csv")
    # generate hyper file
    hyper_file = hyper_auto(df_bitcoin,"bitcoin_price_hist.csv")
    
    # publish hyper file
    publish_files(hyper_file,'Append','myDashBoard')

if __name__ == '__main__':
    main()
