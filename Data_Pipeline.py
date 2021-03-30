import pandas as pd
import pyodbc 
import urllib
import csv
import sqlalchemy as sa
from sqlalchemy import create_engine, event
import datetime

'''
This data pipeline is structured in such a way that whenver it is run, it downloads the data from the link
and looks for any new data. If there is new data, its is added to the table. If the table itself doesn't exists,
then a new table is created & theentire data is inserted into the table.

And the pipeline is designed based on the data that was available in link when it was received.

We will be sticking sticking to the database name 'Ibotta' as it has been used in various queries. 
I haven't had the time to make it dynamic.

For Future Development Points, Kindly read through the queries (SQL File) that I have designed for the analysis.
'''


'''
DATABASE CHECK:
This function will check if there is a database called 'Ibotta', if not, it will create one based on the driver, 
server and the database name provided. 
'''
def database_check(driver, server):
    conn = pyodbc.connect('Driver='+driver+';'
                'Server='+server+';'
                'Trusted_Connection=yes;',
                autocommit=True)
    cursor = conn.cursor()
    print('Checking the availability of the Ibotta database,...')
    cursor.execute("SELECT name FROM master.dbo.sysdatabases where name=?;",(database,))
    data=cursor.fetchall()
    if data:
        print('Database exists')
    else:
        print('Database does not exists.')
        print('Creating new database,...')
        cursor.execute("CREATE DATABASE Ibotta")
        cursor.commit()

'''
CONN SETUP:
This function sets up the connection with the server and returns the SQL server connection object 
linked to the 'Ibotta' database.
'''
def conn_setup(driver,server,database):
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
def cursor_setup(driver,server,database):
    conn = conn_setup(driver,server,database)
    cursor = conn.cursor()
    return cursor

def engine_setup():
    quoted = urllib.parse.quote_plus("DRIVER={SQL Server};SERVER=ramasamy\\sqlexpress;DATABASE=Ibotta; Trusted_Connection=yes;")
    engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(quoted))
    #engine = sa.create_engine('mssql://'+serv+'/'+database+'?trusted_connection=yes')
    return engine



'''
SR & TR DATA READ:
This function will read the Service Requests & Traffic Accidents data from the source and do a little preprocessing based on the issues that I faced while 
playing with it like changing the data type, editing few spelling mistakes. With more time I would be able to make more changes to the data
like mentioning the data types for each and every column so that to speed up the reading process also saving processing time.
'''
def sr_data_read():
    print('Reading Service Requests data,...')
    print('P.S: Ignore any warnings.')
    service_url = 'https://www.denvergov.org/media/gis/DataCatalog/311_service_data/csv/311_service_data_2015.csv'
    sr_data = pd.read_csv(service_url, encoding = "ISO-8859-1")
    #sr_data = pd.read_csv('311_service_data_2015.csv', encoding = "ISO-8859-1")
    sr_data.columns = ['CaseSummary','CaseStatus','CaseSource','CaseCreatedDate','CaseCreatedDttm','CaseClosedDate','CaseClosedDttm','FirstCallResolution','CustomerZipCode','IncidentAddress1','IncidentAddress2','IncidentIntersection1','IncidentIntersection2','IncidentZipCode','Longitude','Latitude','Agency','Division','MajorArea','Type','Topic','CouncilDistrict','PoliceDistrict','Neighborhood']
    sr_data['CaseCreatedDttm'] = pd.to_datetime(sr_data['CaseCreatedDttm'])
    sr_data['CASE_DATE'] = sr_data['CaseCreatedDttm'].dt.date
    sr_data['Type'] = sr_data['Type'].replace('Inqury','Inquiry')
    return sr_data

'''
TR DATA READ:
'''
def tr_data_read():
    print('Reading Traffic Accidents data')
    traffic_url = 'https://www.denvergov.org/media/gis/DataCatalog/traffic_accidents/csv/traffic_accidents.csv'
    tr_data = pd.read_csv(traffic_url, encoding = "ISO-8859-1")
    #tr_data = pd.read_csv('traffic_accidents.csv', encoding = "ISO-8859-1")
    tr_data['FIRST_OCCURRENCE_DATE'] = pd.to_datetime(tr_data['FIRST_OCCURRENCE_DATE'])
    tr_data['INCIDENT_DATE'] = tr_data['FIRST_OCCURRENCE_DATE'].dt.date
    tr_data['LAST_OCCURRENCE_DATE'] = pd.to_datetime(tr_data['LAST_OCCURRENCE_DATE'])
    tr_data['REPORTED_DATE'] = pd.to_datetime(tr_data['REPORTED_DATE'])
    return tr_data

'''
CLEAR TABLES:
The following fuction can be used to drop the Service Requests & Traffic Accidents table from the 
databse. This can be used to clear the data and start freash whenever necessary. 
'''
def clear_tables(driver,server,database):
    cursor = cursor_setup(driver,server,database)
    try:
        cursor.execute("drop table Service_Requests")
    except:
        pass
    try:
        cursor.execute("drop table Traffic_Accidents")
    except:
        pass
    cursor.commit()

'''
CREATE NEW SR TABLE & CREATE NEW TR TABLE:
The following function is used when we need to create a new Service Requests & Traffic Accidents tables and insert all the values that were
read from the source on to the tables. As an added advantage, this function will also create various clustered, nonclustered & filtered indexes that
would optimize the query run time. The design of these indexes are based on my assumption about the frequncy at which the data will be 
accessed and its structure.
'''

def create_new_sr_table(cursor, engine, sr_data):
    cursor.execute("CREATE TABLE Service_Requests (CaseSummary varchar(300),CaseStatus varchar(50),CaseSource varchar(50),CaseCreatedDate datetime,CaseCreatedDttm datetime,CaseClosedDate datetime,CaseClosedDttm datetime,FirstCallResolution varchar(1),CustomerZipCode nvarchar(50),IncidentAddress1 varchar(250),IncidentAddress2 varchar(55),IncidentIntersection1 varchar(50),IncidentIntersection2 varchar(50),IncidentZipCode nvarchar(50),Longitude nvarchar(30),Latitude nvarchar(30),Agency varchar(50),Division varchar(50),MajorArea varchar(50),Type varchar(50),Topic varchar(20),CouncilDistrict nvarchar(30),PoliceDistrict varchar(30),Neighborhood nvarchar(30), CASE_DATE datetime)")
    cursor.execute("CREATE NONCLUSTERED INDEX Latest_Created_Date_Index ON [dbo].[Service_Requests] (CaseCreatedDttm);")
    cursor.commit()
    cursor.execute("CREATE NONCLUSTERED INDEX Inx_One  ON [dbo].[Service_Requests] (CaseCreatedDate,Neighborhood) include (CaseSummary, CaseStatus, CaseSource, IncidentZipCode, Agency, Type);")
    cursor.commit()
    cursor.execute("CREATE NONCLUSTERED INDEX Inx_Two ON [dbo].[Service_Requests] (IncidentZipCode, Type,CaseCreatedDate,CaseClosedDate) include (CaseSummary, CaseStatus, CaseSource,Neighborhood,Agency);")
    cursor.commit()
    cursor.execute("CREATE NONCLUSTERED INDEX Inx_Three ON [dbo].[Service_Requests] (Agency,Division,PoliceDistrict,FirstCallResolution) include (CaseCreatedDate,CaseClosedDate, CaseSummary, CaseStatus, CaseSource,Neighborhood);")
    cursor.commit()
    cursor.execute("CREATE NONCLUSTERED INDEX Inx_Four ON [dbo].[Service_Requests] (CaseCreatedDate,Neighborhood) include (CaseSummary, CaseStatus, CaseSource, IncidentZipCode, Agency, Type) where CaseStatus in ('Escalated','Routed to Agency','In-Progress','New');")
    cursor.commit()
    cursor.execute("CREATE NONCLUSTERED INDEX Inx_Five ON [dbo].[Service_Requests] (IncidentZipCode, Type,CaseCreatedDate,CaseClosedDate) include (CaseSummary, CaseStatus, CaseSource,Neighborhood,Agency) where Type = 'Complaint' and IncidentZipCode is  not null;")
    cursor.commit()
    cursor.execute("CREATE NONCLUSTERED INDEX Inx_Six ON [dbo].[Service_Requests] (Agency,Division,PoliceDistrict,FirstCallResolution) include (CaseCreatedDate,CaseClosedDate, CaseSummary, CaseStatus, CaseSource,Neighborhood) where Agency = '311';")
    cursor.commit()
    cursor.execute("CREATE NONCLUSTERED INDEX Inx_Seven ON [dbo].[Service_Requests] (Type, Neighborhood) include (CouncilDistrict, PoliceDistrict);")
    cursor.commit()
    cursor.execute("CREATE NONCLUSTERED INDEX Inx_Eight ON [dbo].[Service_Requests] (CaseCreatedDttm,CaseClosedDttm, Neighborhood) include (Type, PoliceDistrict, CaseStatus);")
    cursor.commit()
    cursor.execute("CREATE NONCLUSTERED INDEX Inx_Nine ON [dbo].[Service_Requests] (Neighborhood) include (CASE_DATE, CaseCreatedDate, CaseCreatedDttm);")
    cursor.commit()
    print("Table created,..")
    print("Adding "+str(len(sr_data))+" entries to the table")
    begin_time = datetime.datetime.now()
    sr_data.to_sql('Service_Requests',schema='dbo', con = engine, if_exists='append', index=False, chunksize = 1000)
    print("Time taken:"+str(datetime.datetime.now() - begin_time))
    print(str(len(sr_data))+' entries added to the table.')
    print('Table is ready to go!')

def create_new_tr_table(cursor,engine, tr_data):
    cursor.execute("CREATE TABLE Traffic_Accidents (OBJECTID_1 int,INCIDENT_ID bigint,OFFENSE_ID bigint,OFFENSE_CODE int,OFFENSE_CODE_EXTENSION int,TOP_TRAFFIC_ACCIDENT_OFFENSE varchar(100),FIRST_OCCURRENCE_DATE datetime,LAST_OCCURRENCE_DATE datetime,REPORTED_DATE datetime,INCIDENT_ADDRESS varchar(200),GEO_X int,GEO_Y int,GEO_LON decimal(14,10),GEO_LAT decimal(14,10),DISTRICT_ID int,PRECINCT_ID int,NEIGHBORHOOD_ID varchar(100),BICYCLE_IND int,PEDESTRIAN_IND int,HARMFUL_EVENT_SEQ_1 varchar(200),HARMFUL_EVENT_SEQ_2 varchar(200),HARMFUL_EVENT_SEQ_3 varchar(200),ROAD_LOCATION varchar(100),ROAD_DESCRIPTION varchar(200),ROAD_CONTOUR varchar(100),ROAD_CONDITION varchar(100),LIGHT_CONDITION varchar(100),TU1_VEHICLE_TYPE varchar(100),TU1_TRAVEL_DIRECTION varchar(100),TU1_VEHICLE_MOVEMENT varchar(100),TU1_DRIVER_ACTION varchar(100),TU1_DRIVER_HUMANCONTRIBFACTOR varchar(100),TU1_PEDESTRIAN_ACTION varchar(100),TU2_VEHICLE_TYPE varchar(100),TU2_TRAVEL_DIRECTION varchar(100),TU2_VEHICLE_MOVEMENT varchar(100),TU2_DRIVER_ACTION varchar(100),TU2_DRIVER_HUMANCONTRIBFACTOR varchar(100),TU2_PEDESTRIAN_ACTION varchar(100),SERIOUSLY_INJURED int,FATALITIES int,FATALITY_MODE_1 varchar(100),FATALITY_MODE_2 varchar(100),SERIOUSLY_INJURED_MODE_1 varchar(100),SERIOUSLY_INJURED_MODE_2 varchar(100), INCIDENT_DATE datetime)")
    cursor.commit()
    cursor.execute("CREATE CLUSTERED INDEX Inx_ObjectID_FIRST_OCCURRENCE_DATE ON [dbo].[Traffic_Accidents] (OBJECTID_1, FIRST_OCCURRENCE_DATE DESC);")
    cursor.commit()
    cursor.execute("CREATE NONCLUSTERED INDEX NCInx_ObjectID_FIRST_OCCURRENCE_DATE ON [dbo].[Traffic_Accidents] (FIRST_OCCURRENCE_DATE DESC);")
    cursor.commit()
    cursor.execute("CREATE NONCLUSTERED INDEX Inx_One ON [dbo].[Traffic_Accidents] (FIRST_OCCURRENCE_DATE,OFFENSE_CODE,SERIOUSLY_INJURED,FATALITIES) include (INCIDENT_ID,TOP_TRAFFIC_ACCIDENT_OFFENSE,REPORTED_DATE,INCIDENT_ADDRESS);")
    cursor.commit()
    cursor.execute("CREATE NONCLUSTERED INDEX Inx_Two ON [dbo].[Traffic_Accidents] (REPORTED_DATE,OFFENSE_CODE) include (INCIDENT_ID,TOP_TRAFFIC_ACCIDENT_OFFENSE,FIRST_OCCURRENCE_DATE,INCIDENT_ADDRESS,SERIOUSLY_INJURED,FATALITIES);")
    cursor.commit()
    cursor.execute("CREATE NONCLUSTERED INDEX Inx_Three ON [dbo].[Traffic_Accidents] (ROAD_DESCRIPTION,ROAD_CONDITION,LIGHT_CONDITION) include (INCIDENT_ID,FIRST_OCCURRENCE_DATE,REPORTED_DATE,INCIDENT_ADDRESS,PRECINCT_ID,NEIGHBORHOOD_ID);")
    cursor.commit()
    cursor.execute("CREATE NONCLUSTERED INDEX FInx_Four ON [dbo].[Traffic_Accidents] (ROAD_CONDITION,LIGHT_CONDITION) include (INCIDENT_ID,FIRST_OCCURRENCE_DATE,REPORTED_DATE,INCIDENT_ADDRESS,PRECINCT_ID,NEIGHBORHOOD_ID) where ROAD_DESCRIPTION IS NOT NULL AND ROAD_DESCRIPTION IN ('ALLEY RELATED','DRIVEWAY ACCESS RELATED'); ")
    cursor.commit()
    cursor.execute("CREATE NONCLUSTERED INDEX fInx_Five ON [dbo].[Traffic_Accidents] (REPORTED_DATE,OFFENSE_CODE) include (INCIDENT_ID,TOP_TRAFFIC_ACCIDENT_OFFENSE,FIRST_OCCURRENCE_DATE,LAST_OCCURRENCE_DATE,INCIDENT_ADDRESS,SERIOUSLY_INJURED,FATALITIES) WHERE LAST_OCCURRENCE_DATE IS NOT NULL;")
    cursor.commit()
    cursor.execute("CREATE NONCLUSTERED INDEX Inx_Six ON [dbo].[Traffic_Accidents] (TU1_DRIVER_HUMANCONTRIBFACTOR) include (INCIDENT_ID,TOP_TRAFFIC_ACCIDENT_OFFENSE,FIRST_OCCURRENCE_DATE,INCIDENT_ADDRESS,SERIOUSLY_INJURED,FATALITIES) WHERE LAST_OCCURRENCE_DATE IS NOT NULL;")
    cursor.commit()
    cursor.execute("CREATE NONCLUSTERED INDEX Inx_Seven ON [dbo].[Traffic_Accidents] (TU2_DRIVER_HUMANCONTRIBFACTOR) include (INCIDENT_ID,TOP_TRAFFIC_ACCIDENT_OFFENSE,FIRST_OCCURRENCE_DATE,INCIDENT_ADDRESS,SERIOUSLY_INJURED,FATALITIES) WHERE LAST_OCCURRENCE_DATE IS NOT NULL;")
    cursor.commit()
    cursor.execute("CREATE NONCLUSTERED INDEX Inx_Eight ON [dbo].[Traffic_Accidents] (NEIGHBORHOOD_ID) include (INCIDENT_ID,TOP_TRAFFIC_ACCIDENT_OFFENSE,FIRST_OCCURRENCE_DATE,INCIDENT_ADDRESS,PRECINCT_ID);")
    cursor.commit()
    cursor.execute("CREATE NONCLUSTERED INDEX Inx_Nine ON [dbo].[Traffic_Accidents] (FIRST_OCCURRENCE_DATE,REPORTED_DATE,NEIGHBORHOOD_ID) include (INCIDENT_DATE);")
    cursor.commit()
    print("Table created")
    print("Adding "+str(len(tr_data))+" entries to the table")
    begin_time = datetime.datetime.now()
    tr_data.to_sql('Traffic_Accidents',schema='dbo', con = engine, if_exists='append', index=False, chunksize = 1000)
    print("Time taken:"+str(datetime.datetime.now() - begin_time))
    print(str(len(tr_data))+' entries added to the table.')
    


'''
ADD NEW SR DATA & ADD NEW TR DATA:
The following two functions are used to add and new data that has been found at the source. This is done by comparing the 'CaseCreatedDttm'
from the Service Request table and 'FIRST_OCCURRENCE_DATE' from the Traffic Accidents table and their reqpective sources. And add the new data onto the
table.
'''

def add_new_sr_data(cursor,engine,sr_data):
    case_date_table = cursor.execute("select top 1 CaseCreatedDttm from [dbo].[Service_Requests] order by CaseCreatedDttm desc")
    case_date_csv = max(sr_data['CaseCreatedDttm'])
    #test_date = '2015-12-30 03:59:11'
    #test_date = datetime.datetime.strptime(case_date_csv, '%Y-%m-%d %H:%M:%S')
    for x in case_date_table:
        table_date = x
    if table_date[0] >= case_date_csv:
    #if test_date >= case_date_csv:
        print('Table is up-to-date!!')
    else:
        print('New Data Available!!')
        New_Data = sr_data[sr_data['CaseCreatedDttm']>table_date[0]]
        print("Inserting new data to the table,..")
        print("Adding "+str(len(New_data))+" entries to the table")
        begin_time = datetime.datetime.now()
        New_Data.to_sql('Service_Requests',schema='dbo', con = engine, if_exists='append', index=False, chunksize = 1000)
        print("Time taken:"+str(datetime.datetime.now() - begin_time))
        print(str(len(New_data))+' entries added to the table.')
        print('Table is ready!')

def add_new_tr_data(cursor, engine,sr_data):
    occurence_date_table = cursor.execute("select top 1 FIRST_OCCURRENCE_DATE from [dbo].[Traffic_Accidents] order by REPORTED_DATE desc")
    case_date_csv = max(tr_data['FIRST_OCCURRENCE_DATE'])
    #test_date = '2015-12-30 03:59:11'
    #test_date = datetime.datetime.strptime(case_date_csv, '%Y-%m-%d %H:%M:%S')
    for x in occurence_date_table:
        table_date = x
    if table_date[0] >= case_date_csv:
    #if test_date >= case_date_csv:
        print('Table is up-to-date!!')
    else:
        print('New Data Available!!')
        New_Data = tr_data[tr_data['FIRST_OCCURRENCE_DATE']>table_date[0]]
        print("Inserting new data to the table,..")
        print("Adding "+str(len(New_data))+" entries to the table")
        begin_time = datetime.datetime.now()
        New_Data.to_sql('Traffic_Accidents',schema='dbo', con = engine, if_exists='append', index=False, chunksize = 1000)
        print("Time taken:"+str(datetime.datetime.now() - begin_time))
        print(str(len(New_data))+' entries added to the table.')

'''
SR TABLE CHECK & TR TABLE CHECK:
The following two functions will check if the tables Service Requests & traffic Accidents already exists. If yes, will heck for new data, 
if not, will go ahead and create a new table and add all the data from the source onto their respective tables.
'''
def sr_table_check(driver,server,database,sr_data):
    cursor = cursor_setup(driver,server,database)
    engine = engine_setup() 
    print('Checking the existence of the Service_Requests table.')
    if cursor.tables(table='Service_Requests', tableType='TABLE').fetchone():
        print("Table exists!")
        add_new_sr_data(cursor,engine,sr_data)
    else:
        print("table doesn't exist,... creating the table")
        create_new_sr_table(cursor,engine ,sr_data)

def tr_table_check(driver,server,database,tr_data):
    print('Checking the existence of the Traffic_Accidents table.')
    cursor = cursor_setup(driver,server,database)
    engine = engine_setup() 
    if cursor.tables(table='Traffic_Accidents', tableType='TABLE').fetchone():
        print("Table exists")
        add_new_tr_data(cursor,engine ,tr_data)
    else:
        print("Table doesn't exist,... creating the table")
        create_new_tr_table(cursor,engine, tr_data)
        



if __name__ == "__main__":
    database = 'Ibotta'

    '''
    Kindly replace the names of the following variables (like Driver and the default SQL Server
    that you use) as per your system requirements.
    '''

    #username = 'RAMASAMY\Sanjeev'
    driver = '{SQL Server}'
    server = 'ramasamy\sqlexpress' 

    #Comment the below line after the first time to avoid deleting the tables.
    clear_tables(driver,server,database)
    
    #Checking database existence
    database_check(driver, server)

    #Reading data
    sr_data = sr_data_read()
    tr_data = tr_data_read()

    #Checking the existence of the tables/new data
    sr_table_check(driver,server,database,sr_data)
    tr_table_check(driver,server,database,tr_data)
    print("All process completed. Have fub!")












# u = {}
# for x in sr_data.columns:
#     u[str(x)] = len(sr_data[str(x)].unique())

# sr_data['Type'].unique()

# {k: v for k, v in sorted(u.items(), key=lambda item: item[1])}

# Queries for SR:
"""
cursor.execute("CREATE NONCLUSTERED INDEX Latest_Created_Date_Index  
ON [dbo].[Service_Requests] (CaseCreatedDttm);
")
--CREATE CLUSTERED INDEX Inx_Created_Date  
--ON [dbo].[Service_Requests] (CaseCreatedDttm);

cursor.execute("CREATE NONCLUSTERED INDEX Inx_One  
ON [dbo].[Service_Requests] (CaseCreatedDate,Neighborhood) include (CaseSummary, CaseStatus, CaseSource, IncidentZipCode, Agency, Type);
")
--select CaseSummary, CaseStatus, CaseSource, IncidentZipCode, Agency, Type from Service_Requests where CaseCreatedDate>'2015/10/12' and Neighborhood is not null and IncidentZipCode = '80223-2523'

cursor.execute("CREATE NONCLUSTERED INDEX Inx_Two  
ON [dbo].[Service_Requests] (IncidentZipCode, Type,CaseCreatedDate,CaseClosedDate) include (CaseSummary, CaseStatus, CaseSource,Neighborhood,Agency);
")
--select CaseSummary, CaseStatus, CaseSource,Neighborhood,Agency from Service_Requests where CaseCreatedDate>'2015/10/12' and CaseClosedDate< '2015/10/13' and Type = 'Complaint' and IncidentZipCode = '80223-2523'

cursor.execute("CREATE NONCLUSTERED INDEX Inx_Three  
ON [dbo].[Service_Requests] (Agency,Division,PoliceDistrict,FirstCallResolution) include (CaseCreatedDate,CaseClosedDate, CaseSummary, CaseStatus, CaseSource,Neighborhood);
")

cursor.execute("CREATE NONCLUSTERED INDEX Inx_Four  
ON [dbo].[Service_Requests] (CaseCreatedDate,Neighborhood) include (CaseSummary, CaseStatus, CaseSource, IncidentZipCode, Agency, Type) where CaseStatus in ('Escalated','Routed to Agency','In-Progress','New');
")
--select CaseSummary, CaseStatus, CaseSource, IncidentZipCode, Agency, Type from Service_Requests where CaseCreatedDate>'2015/10/12' and Neighborhood is not null and CaseStatus in ('Escalated','Routed to Agency','In-Progress')

cursor.execute("CREATE NONCLUSTERED INDEX Inx_Five  
ON [dbo].[Service_Requests] (IncidentZipCode, Type,CaseCreatedDate,CaseClosedDate) include (CaseSummary, CaseStatus, CaseSource,Neighborhood,Agency) where Type = 'Complaint' and IncidentZipCode is  not null;
")
--select CaseSummary, CaseStatus, CaseSource,Neighborhood,IncidentZipCode,Agency from Service_Requests where CaseCreatedDate>'2015/10/12' and Type = 'Complaint'

cursor.execute("CREATE NONCLUSTERED INDEX Inx_Six  
ON [dbo].[Service_Requests] (Agency,Division,PoliceDistrict,FirstCallResolution) include (CaseCreatedDate,CaseClosedDate, CaseSummary, CaseStatus, CaseSource,Neighborhood) where Agency = '311';
")
--select CaseSummary, CaseStatus, CaseSource,Neighborhood from Service_Requests where Type = 'Complaint' AND Agency = '311'

cursor.execute("CREATE NONCLUSTERED INDEX Inx_Seven ON [dbo].[Service_Requests] (Type, Neighborhood) include (CouncilDistrict, PoliceDistrict);")

cursor.execute("CREATE NONCLUSTERED INDEX Inx_Eight ON [dbo].[Service_Requests] (CaseCreatedDttm,CaseClosedDttm, Neighborhood) include (Type, PoliceDistrict, CaseStatus);")

cursor.execute("CREATE NONCLUSTERED INDEX Inx_Nine ON [dbo].[Service_Requests] (Neighborhood) include (CASE_DATE, CaseCreatedDate, CaseCreatedDttm);")
"""
#Queries for TR:
# t = {}
# for x in tr_data.columns:
#     t[str(x)] = len(tr_data[str(x)].unique())

# {k: v for k, v in sorted(t.items(), key=lambda item: item[1])}

'''
cursor.execute("CREATE CLUSTERED INDEX Inx_ObjectID_REPORTED_DATE ON [dbo].[Traffic_Accidents] (OBJECTID_1, REPORTED_DATE DESC);")
cursor.execute("CREATE NONCLUSTERED INDEX NCInx_ObjectID_REPORTED_DATE ON [dbo].[Traffic_Accidents] (REPORTED_DATE DESC);")
cursor.execute("CREATE NONCLUSTERED INDEX Inx_One ON [dbo].[Traffic_Accidents] (FIRST_OCCURRENCE_DATE,OFFENSE_CODE,SERIOUSLY_INJURED,FATALITIES) include (INCIDENT_ID,TOP_TRAFFIC_ACCIDENT_OFFENSE,REPORTED_DATE,INCIDENT_ADDRESS);")

--select INCIDENT_ID,FIRST_OCCURRENCE_DATE,TOP_TRAFFIC_ACCIDENT_OFFENSE,REPORTED_DATE,INCIDENT_ADDRESS,SERIOUSLY_INJURED,FATALITIES from Traffic_Accidents where FIRST_OCCURRENCE_DATE>'2019-03-12' and OFFENSE_CODE = 5401

cursor.execute("CREATE NONCLUSTERED INDEX Inx_Two ON [dbo].[Traffic_Accidents] (REPORTED_DATE,OFFENSE_CODE) include (INCIDENT_ID,TOP_TRAFFIC_ACCIDENT_OFFENSE,FIRST_OCCURRENCE_DATE,INCIDENT_ADDRESS,SERIOUSLY_INJURED,FATALITIES);")

--select INCIDENT_ID,TOP_TRAFFIC_ACCIDENT_OFFENSE,FIRST_OCCURRENCE_DATE,INCIDENT_ADDRESS,SERIOUSLY_INJURED,FATALITIES from [dbo].[Traffic_Accidents] where REPORTED_DATE> '2019-12-12' and OFFENSE_CODE<5500

cursor.execute("CREATE NONCLUSTERED INDEX Inx_Three ON [dbo].[Traffic_Accidents] (ROAD_DESCRIPTION,ROAD_CONDITION,LIGHT_CONDITION) include (INCIDENT_ID,FIRST_OCCURRENCE_DATE,REPORTED_DATE,INCIDENT_ADDRESS,PRECINCT_ID,NEIGHBORHOOD_ID);")

--select INCIDENT_ID,FIRST_OCCURRENCE_DATE,REPORTED_DATE,INCIDENT_ADDRESS,PRECINCT_ID,NEIGHBORHOOD_ID from [dbo].[Traffic_Accidents] where ROAD_DESCRIPTION = 'AT INTERSECTION' and ROAD_CONDITION = 'SNOWY' or LIGHT_CONDITION = 'DAY LIGHT'

cursor.execute("CREATE NONCLUSTERED INDEX FInx_Four ON [dbo].[Traffic_Accidents] (ROAD_CONDITION,LIGHT_CONDITION) include (INCIDENT_ID,FIRST_OCCURRENCE_DATE,REPORTED_DATE,INCIDENT_ADDRESS,PRECINCT_ID,NEIGHBORHOOD_ID) where ROAD_DESCRIPTION IS NOT NULL AND ROAD_DESCRIPTION IN ('ALLEY RELATED','DRIVEWAY ACCESS RELATED'); ")
--select INCIDENT_ID,FIRST_OCCURRENCE_DATE,REPORTED_DATE,INCIDENT_ADDRESS,PRECINCT_ID,NEIGHBORHOOD_ID from [dbo].[Traffic_Accidents] where ROAD_DESCRIPTION = 'DRIVEWAY ACCESS RELATED' and ROAD_CONDITION = 'SNOWY'

cursor.execute("CREATE NONCLUSTERED INDEX fInx_Five ON [dbo].[Traffic_Accidents] (REPORTED_DATE,OFFENSE_CODE) include (INCIDENT_ID,TOP_TRAFFIC_ACCIDENT_OFFENSE,FIRST_OCCURRENCE_DATE,LAST_OCCURRENCE_DATE,INCIDENT_ADDRESS,SERIOUSLY_INJURED,FATALITIES) WHERE LAST_OCCURRENCE_DATE IS NOT NULL;")
--select INCIDENT_ID,TOP_TRAFFIC_ACCIDENT_OFFENSE,FIRST_OCCURRENCE_DATE,LAST_OCCURRENCE_DATE,INCIDENT_ADDRESS,SERIOUSLY_INJURED,FATALITIES from [dbo].[Traffic_Accidents] where LAST_OCCURRENCE_DATE IS NOT NULL AND REPORTED_DATE> '2019-12-12' and OFFENSE_CODE<5500

cursor.execute("CREATE NONCLUSTERED INDEX Inx_Six ON [dbo].[Traffic_Accidents] (TU1_DRIVER_HUMANCONTRIBFACTOR) include (INCIDENT_ID,TOP_TRAFFIC_ACCIDENT_OFFENSE,FIRST_OCCURRENCE_DATE,INCIDENT_ADDRESS,SERIOUSLY_INJURED,FATALITIES) WHERE LAST_OCCURRENCE_DATE IS NOT NULL;")

cursor.execute("CREATE NONCLUSTERED INDEX Inx_Seven ON [dbo].[Traffic_Accidents] (TU2_DRIVER_HUMANCONTRIBFACTOR) include (INCIDENT_ID,TOP_TRAFFIC_ACCIDENT_OFFENSE,FIRST_OCCURRENCE_DATE,INCIDENT_ADDRESS,SERIOUSLY_INJURED,FATALITIES) WHERE LAST_OCCURRENCE_DATE IS NOT NULL;")

cursor.execute("CREATE NONCLUSTERED INDEX Inx_Eight ON [dbo].[Traffic_Accidents] (NEIGHBORHOOD_ID) include (INCIDENT_ID,TOP_TRAFFIC_ACCIDENT_OFFENSE,FIRST_OCCURRENCE_DATE,INCIDENT_ADDRESS,PRECINCT_ID);")

cursor.execute("CREATE NONCLUSTERED INDEX Inx_Nine ON [dbo].[Traffic_Accidents] (FIRST_OCCURRENCE_DATE,REPORTED_DATE,NEIGHBORHOOD_ID) include (INCIDENT_DATE);")

'''


# cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database)
# cursor = cnxn.cursor()


# x = cursor.execute('SELECT * FROM Service_Requests')
# for y in x:
#     print(x)

# quoted = urllib.parse.quote_plus("SERVER=ramasamy\sqlexpress;DATABASE=Ibotto; Trusted_Connection=yes;")
# engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(quoted))
# connection = engine.connect()



 

# quoted = urllib.parse.quote_plus("DRIVER={SQL Server};SERVER=ramasamy\\sqlexpress;DATABASE=Ibotta; Trusted_Connection=yes;")
# engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(quoted))
# connection = engine.connect()
# res = connection.execute("select * from Ibotta.dbo.Service_Requests")
# for x in res:
#     print(x)



#sr_data.head(5)



# conn = pyodbc.connect('Driver={SQL Server};'
#                       'Server=ramasamy\sqlexpress;'
#                       'Trusted_Connection=yes;'
#                       'Database=Ibotta',
#                       autocommit=True)
# cursor = conn.cursor()


# params = 'DRIVER='+driver + ';SERVER='+server + ';PORT=1433;DATABASE=' + database
# db_params = urllib.parse.quote_plus(params)
# #engine = sqlalchemy.create_engine("mssql+pyodbc:///?odbc_connect={}".format(db_params))
# @event.listens_for(engine, "before_cursor_execute")
# def receive_before_cursor_execute(conn, cursor, statement, params, context, executemany):
#             if executemany:
#                 cursor.fast_executemany = True

# sr_data.to_sql('Service_Requests',engine,index=False,if_exists="append", chunksize = 1000)






