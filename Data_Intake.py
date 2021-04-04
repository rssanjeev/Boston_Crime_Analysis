import urllib
import requests
import json
import pandas as pd
from pandas.io.json import json_normalize
import pyodbc 
import csv, time
import sqlalchemy as sa
from sqlalchemy import create_engine, event
import datetime

class Data_Intake:

    def __init__(self, driver, server):
            self.driver = driver
            self.server = server

            '''
    CONN SETUP:
    This function sets up the connection with the server and returns the SQL server connection object 
    linked to the 'Ibotta' database.
    '''
    def conn_setup(self, driver,server,database):
        conn = pyodbc.connect('Driver='+driver+';'
                        'Server='+server+';'
                        'Trusted_Connection=yes;'
                        'Database='+database+'',
                        autocommit=True)
        return conn


    '''
    CURSOR & ENGINE SETUP:
    The next two functions are used to setup the connection with the SQl server that would enable us to 
    execute SQL queries and commit the changes to the databse table.
    '''
    def cursor_setup(self, driver,server,database):
        conn = conn_setup(driver,server,database)
        cursor = conn.cursor()
        return cursor

    def engine_setup(self):
        quoted = urllib.parse.quote_plus("DRIVER={SQL Server};SERVER=ramasamy\\sqlexpress;DATABASE=Boston_Crime; Trusted_Connection=yes;")
        engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(quoted))
        #engine = sa.create_engine('mssql://'+serv+'/'+database+'?trusted_connection=yes')
        return engine

    def clear_tables(self, driver,server,database):
        cursor = cursor_setup(driver,server,database)
        try:
            cursor.execute("drop table Crimes")
            cursor.commit()
        except:
            pass

    def first_data_read(self, cursor):
        print('Reading Boston Crime data,...')
        print('P.S: Ignore any warnings.')
        #service_url = 'https://www.denvergov.org/media/gis/DataCatalog/311_service_data/csv/311_service_data_2015.csv'
        #data = pd.read_csv('https://data.boston.gov/datastore/dump/12cb3883-56f5-47de-afa5-3b1cf61b257b')
        data_2021 = pd.read_csv('https://data.boston.gov/datastore/dump/3d818157-6e9b-4fa5-86de-436ca663d88e')
        data_2020 = pd.read_csv('https://data.boston.gov/datastore/dump/bc85f079-e36f-4269-9c07-e47a40f0976b')
        data_2019 = pd.read_csv('https://data.boston.gov/datastore/dump/54e0eb09-c012-429f-90e8-8c5f2694ce27')
        data_2018 = pd.read_csv('https://data.boston.gov/datastore/dump/e86f8e38-a23c-4c1a-8455-c8f94210a8f1')
        data_2017 = pd.read_csv('https://data.boston.gov/datastore/dump/64ad0053-842c-459b-9833-ff53d568f2e3')
        data_2016 = pd.read_csv('https://data.boston.gov/datastore/dump/b6c4e2c3-7b1e-4f4a-b019-bef8c6a0e882')
        data_2015 = pd.read_csv('https://data.boston.gov/datastore/dump/792031bf-b9bb-467c-b118-fe795befdf00')
        pdList = [data_2015, data_2016, data_2017, data_2018, data_2019, data_2020, data_2021]
        data = pd.concat(pdList)
        data.drop(['Location'],axis = 1, inplace=True)
        return data


    def new_data_check(self, cursor):
        print("Checking for new data,...")
        #new_data = first_data_read(cursor)
        x = cursor.execute("select max(OCCURRED_ON_DATE) from Crimes")
        temp = x.fetchall()
        recent_timestamp = temp[0][0]
        data_2021 = pd.read_csv('https://data.boston.gov/datastore/dump/3d818157-6e9b-4fa5-86de-436ca663d88e')
        #time.sleep(5)
        data_2021['OCCURRED_ON_DATE'] =  pd.to_datetime(data_2021['OCCURRED_ON_DATE'])
        if recent_timestamp>=data_2021['OCCURRED_ON_DATE'].max():
            print('No new data is available.')
            print("Table is up-to-date!")        
        else:
            print('New data available, preparing to upload new data.')
            new_data = data_2021[data_2021['OCCURRED_ON_DATE']>recent_timestamp]
            new_data.to_sql('Crimes',schema='dbo', con = engine, if_exists='append', index=False, chunksize = 10000)
            print('Table is ready to rock n roll!!')


    def database_check(self, driver, server, engine):
        conn = pyodbc.connect('Driver='+driver+';'
                    'Server='+server+';'
                    'Trusted_Connection=yes;',
                    autocommit=True)
        cursor = conn.cursor()
        print('Checking the availability of the Boston_Crime database,...')
        database = 'Boston_Crime'
        cursor.execute("SELECT name FROM master.dbo.sysdatabases where name=?;",(database,))
        data=cursor.fetchall()
        if data:
            print('Database exists')
            print('Checking for table,..')
            if cursor.tables(table='Crimes', tableType='TABLE').fetchone():
                print("Table exists!")
                db = 'Boston_Crime'
                conn = pyodbc.connect('Driver='+driver+';'
                'Server='+server+';'
                'Trusted_Connection=yes;'
                'Database='+db+'',
                autocommit=True)
                #cursor = cursor_setup(driver,server,db)
                cursor = conn.cursor()
                print("Checking for new data,...")
                #new_data = first_data_read(cursor)
                x = cursor.execute("select max(OCCURRED_ON_DATE) from Crimes")
                temp = x.fetchall()
                recent_timestamp = temp[0][0]
                data_2021 = pd.read_csv('https://data.boston.gov/datastore/dump/3d818157-6e9b-4fa5-86de-436ca663d88e')
                #time.sleep(5)
                data_2021['OCCURRED_ON_DATE'] =  pd.to_datetime(data_2021['OCCURRED_ON_DATE'])
                if recent_timestamp>=data_2021['OCCURRED_ON_DATE'].max():
                    print('No new data is available.')
                    print("Table is up-to-date!")        
                else:
                    print('New data available, preparing to upload new data.')
                    new_data_temp = data_2021[data_2021['OCCURRED_ON_DATE']>recent_timestamp]
                    off_data = pd.read_excel('https://data.boston.gov/dataset/6220d948-eae2-4e4b-8723-2dc8e67722a3/resource/3aeccf51-a231-4555-ba21-74572b4c33d6/download/rmsoffensecodes.xlsx')
                    off_data.columns = ['CODE', 'OFFENSE']
                    new_data_temp['OFFENSE_CODE'] = pd.to_numeric(new_data_temp['OFFENSE_CODE'])
                    new_data = pd.merge(new_data_temp, off_data, left_on='OFFENSE_CODE', right_on = 'CODE')
                    new_data.drop(['CODE'],axis = 1, inplace=True)
                    new_data.drop(['Location'],axis = 1, inplace=True)
                    new_data.to_sql('Crimes',schema='dbo', con = engine, if_exists='append', index=False, chunksize = 10000)
                    print('Table is ready to rock n roll!!')
                #new_data_check(cursor)
                #print('Table is up-to-date and ready!')
                return conn
        else:
            print('Database does not exists.')
            print('Creating new database,...')
            cursor.execute("CREATE DATABASE Boston_Crime")
            cursor.commit()
            #data = first_data_read(cursor)
            print('Reading Boston Crime data,...')
            print('P.S: Ignore any warnings.')
            #service_url = 'https://www.denvergov.org/media/gis/DataCatalog/311_service_data/csv/311_service_data_2015.csv'
            #data = pd.read_csv('https://data.boston.gov/datastore/dump/12cb3883-56f5-47de-afa5-3b1cf61b257b')
            data_2021 = pd.read_csv('https://data.boston.gov/datastore/dump/3d818157-6e9b-4fa5-86de-436ca663d88e')
            data_2020 = pd.read_csv('https://data.boston.gov/datastore/dump/bc85f079-e36f-4269-9c07-e47a40f0976b')
            data_2019 = pd.read_csv('https://data.boston.gov/datastore/dump/54e0eb09-c012-429f-90e8-8c5f2694ce27')
            data_2018 = pd.read_csv('https://data.boston.gov/datastore/dump/e86f8e38-a23c-4c1a-8455-c8f94210a8f1')
            data_2017 = pd.read_csv('https://data.boston.gov/datastore/dump/64ad0053-842c-459b-9833-ff53d568f2e3')
            data_2016 = pd.read_csv('https://data.boston.gov/datastore/dump/b6c4e2c3-7b1e-4f4a-b019-bef8c6a0e882')
            data_2015 = pd.read_csv('https://data.boston.gov/datastore/dump/792031bf-b9bb-467c-b118-fe795befdf00')
            pdList = [data_2015, data_2016, data_2017, data_2018, data_2019, data_2020, data_2021]
            data_temp = pd.concat(pdList)
            data_temp.drop(['Location'],axis = 1, inplace=True)
            off_data = pd.read_excel('https://data.boston.gov/dataset/6220d948-eae2-4e4b-8723-2dc8e67722a3/resource/3aeccf51-a231-4555-ba21-74572b4c33d6/download/rmsoffensecodes.xlsx')
            off_data.columns = ['CODE', 'OFFENSE']
            data_temp['OFFENSE_CODE'] = pd.to_numeric(data_temp['OFFENSE_CODE'])
            data = pd.merge(data_temp, off_data, left_on='OFFENSE_CODE', right_on = 'CODE')
            data.drop(['CODE'],axis = 1, inplace=True)

            db = 'Boston_Crime'
            conn = pyodbc.connect('Driver='+driver+';'
            'Server='+server+';'
            'Trusted_Connection=yes;'
            'Database='+db+'',
            autocommit=True)
            #cursor = cursor_setup(driver,server,db)
            cursor = conn.cursor()
            print('Creating new table,..')
            cursor.execute("CREATE TABLE Crimes ([INCIDENT_NUMBER] [varchar](50) NOT NULL, [OFFENSE_CODE][varchar](50) NULL,[OFFENSE] [VARCHAR] (200),[OFFENSE_CODE_GROUP][varchar](200) NULL,[OFFENSE_DESCRIPTION][varchar](300) NULL,[DISTRICT] [varchar](30) NULL,[REPORTING_AREA] [varchar](50) NULL,[SHOOTING][VARCHAR](30) NULL,[OCCURRED_ON_DATE] [datetime] NULL, [YEAR] [int],[MONTH] [INT],DAY_OF_WEEK [VARCHAR](15), [HOUR] [INT],[UCR_PART] [varchar](50) NULL,[STREET] [varchar](100) NULL, [Lat] [DECIMAL](12,9), [Long] [DECIMAL](12,9))")
            cursor.commit()
            print('Adding data to the new table!')
            data.to_sql('Crimes',schema='dbo', con = engine, if_exists='append', index=False, chunksize = 10000)
            print('Table is up-to-date and ready!')
            return conn


    # def data_intake(self, driver, server, engine):
    #     #database = 'Boston_Crime'
    #     '''
    #     Kindly replace the names of the following variables (like Driver and the default SQL Server
    #     that you use) as per your system requirements.
    #     '''
        
    #     #Checking database existence
    #     database_check(driver, server, engine)

    #     #Comment the below line after the first time to avoid deleting the tables.
    #     #clear_tables(driver,server,database)


