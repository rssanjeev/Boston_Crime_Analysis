from Data_Intake import *
import pyodbc 
import csv, time
import sqlalchemy as sa
from sqlalchemy import create_engine, event
import datetime
import numpy as np

import torch
import torch.nn as nn
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
#%matplotlib inline


def data_extract():
        #cursor = conn.cursor()
        db = 'Boston_Crime'
        conn = pyodbc.connect('Driver='+driver+';'
        'Server='+server+';'
        'Trusted_Connection=yes;'
        'Database='+db+'',
        autocommit=True)
        sql = "select * from Crimes order by OCCURRED_ON_DATE desc"
        data = pd.read_sql(sql,conn)
        return data

if __name__ == '__main__':
        driver = '{SQL Server}'
        server = 'ramasamy\sqlexpress'
        d = Data_Intake(driver, server)
        engine = d.engine_setup()
        #try:
        conn = d.database_check(driver, server, engine)
        data = data_extract()
        time.sleep(10)
        #except:
        print('Check the code!')
        sub_data = data[['OFFENSE_CODE','DISTRICT', 'REPORTING_AREA', 'SHOOTING','OCCURRED_ON_DATE', 'YEAR', 'MONTH', 'DAY_OF_WEEK', 'HOUR']]
        sub_data['DATE'] = sub_data['OCCURRED_ON_DATE'].dt.day
        sub_data['SHOOTING'] = sub_data['SHOOTING'].map({'Y': 1, 'None': 0})
        sub_data = sub_data.fillna(0)
        sub_data.drop(['OCCURRED_ON_DATE'],axis = 1, inplace=True)

        fraud_codes = ['1102','1105','1106','1107','1108','1109']
        fraud_data = sub_data[sub_data['OFFENSE_CODE'].isin(fraud_codes)]
        fraud_data['OFFENSE_CODE'] = 'FRAUD'


        assault_codes = ['402','403','413','423','432','801','802']
        assault_data = sub_data[sub_data['OFFENSE_CODE'].isin(assault_codes)]
        assault_data['OFFENSE_CODE'] = 'ASSAULT'


        burglary_codes = ['520','521','522','540','541','542','560','561','562']
        burglary_data = sub_data[sub_data['OFFENSE_CODE'].isin(burglary_codes)]
        burglary_data['OFFENSE_CODE'] = 'BURGLARY'


        larceny_codes = ['611','612','613','614','615','616','617','618','619','623','624','627','629','633','634','637','639']
        larceny_data = sub_data[sub_data['OFFENSE_CODE'].isin(larceny_codes)]
        larceny_data['OFFENSE_CODE'] = 'LARCENY'


        df = fraud_data.append([assault_data,burglary_data,larceny_data ])
        df = df.replace(r'^\s*$', np.nan, regex=True)
        df = df.fillna(0)
        df['OFFENSE_CODE'] = df['OFFENSE_CODE'].replace({'ASSAULT':1, 'BURGLARY':2, 'FRAUD':3,'LARCENY':4})
        df['REPORTING_AREA'] = pd.to_numeric(df['REPORTING_AREA'])
        df['SHOOTING'] = df['SHOOTING'].astype('Int64')

        dataset = df

        categorical_columns = ['DISTRICT', 'SHOOTING','YEAR','MONTH','DAY_OF_WEEK',]

        numerical_columns = ['REPORTING_AREA','HOUR','DATE']

        outputs = ['OFFENSE_CODE']

        for category in categorical_columns:
                dataset[category] = dataset[category].astype('category')

        DAY = dataset['DAY_OF_WEEK'].cat.codes.values
        DISTRICT = dataset['DISTRICT'].cat.codes.values
        YEAR = dataset['YEAR'].cat.codes.values
        MONTH = dataset['MONTH'].cat.codes.values
        #HOUR = dataset['HOUR'].cat.codes.values
        #DATE = dataset['DATE'].cat.codes.values


        categorical_data = np.stack([DAY,DISTRICT,YEAR,MONTH,HOUR,DATE],1)

        categorical_data = torch.tensor(categorical_data, dtype=torch.int64)
        categorical_data[:10]

        numerical_data = np.stack([dataset[col].values for col in numerical_columns], 1)
        numerical_data = torch.tensor(numerical_data, dtype=torch.float)

        outputs = torch.tensor(dataset[outputs].values).flatten()

        #Embedding
        categorical_column_sizes = [len(dataset[column].cat.categories) for column in categorical_columns]
        categorical_embedding_sizes = [(col_size, min(50, (col_size+1)//2)) for col_size in categorical_column_sizes]

        #Data Split
        total_records = 1869260
        test_records = int(total_records * .2)

        categorical_train_data = categorical_data[:total_records-test_records]
        categorical_test_data = categorical_data[total_records-test_records:total_records]
        numerical_train_data = numerical_data[:total_records-test_records]
        numerical_test_data = numerical_data[total_records-test_records:total_records]
        train_outputs = outputs[:total_records-test_records]
        test_outputs = outputs[total_records-test_records:total_records]


        class Model(nn.Module):

                def __init__(self, embedding_size, num_numerical_cols, output_size, layers, p=0.4):
                        super().__init__()
                        self.all_embeddings = nn.ModuleList([nn.Embedding(ni, nf) for ni, nf in embedding_size])
                        self.embedding_dropout = nn.Dropout(p)
                        self.batch_norm_num = nn.BatchNorm1d(num_numerical_cols)

                        all_layers = []
                        num_categorical_cols = sum((nf for ni, nf in embedding_size))
                        input_size = num_categorical_cols + num_numerical_cols

                        for i in layers:
                                all_layers.append(nn.Linear(input_size, i))
                                all_layers.append(nn.ReLU(inplace=True))
                                all_layers.append(nn.BatchNorm1d(i))
                                all_layers.append(nn.Dropout(p))
                                input_size = i

                        all_layers.append(nn.Linear(layers[-1], output_size))

                        self.layers = nn.Sequential(*all_layers)

                def forward(self, x_categorical, x_numerical):
                        embeddings = []
                        for i,e in enumerate(self.all_embeddings):
                                embeddings.append(e(x_categorical[:,i]))
                        x = torch.cat(embeddings, 1)
                        x = self.embedding_dropout(x)

                        x_numerical = self.batch_norm_num(x_numerical)
                        x = torch.cat([x, x_numerical], 1)
                        x = self.layers(x)
                        return x

        model = Model(categorical_embedding_sizes, numerical_data.shape[1], 4, [200,100,50], p=0.4)

        loss_function = nn.CrossEntropyLoss()
        optimizer = torch.optim.Adam(model.parameters(), lr=0.001)


        epochs = 300
        aggregated_losses = []

        for i in range(epochs):
                i += 1
                y_pred = model(categorical_train_data, numerical_train_data)
                single_loss = loss_function(y_pred, train_outputs)
                aggregated_losses.append(single_loss)

                if i%25 == 1:
                        print(f'epoch: {i:3} loss: {single_loss.item():10.8f}')

                optimizer.zero_grad()
                single_loss.backward()
                optimizer.step()

        print(f'epoch: {i:3} loss: {single_loss.item():10.10f}')




# print(len(categorical_train_data))
# print(len(numerical_train_data))
# print(len(train_outputs))

# print(len(categorical_test_data))
# print(len(numerical_test_data))
# print(len(test_outputs))





















#Rough
unique_reportingares = set(df['REPORTING_AREA'])
unique_districts = set(df['DISTRICT'])
unique_offense = set(df['OFFENSE_CODE'])
unique_shooting = set(df['SHOOTING'])
unique_year = set(df['YEAR'])
unique_MONTH = set(df['MONTH'])
unique_DAY_OF_WEEK = set(df['DAY_OF_WEEK'])
unique_HOUR = set(df['HOUR'])
unique_DATE = set(df['DATE'])




pdList = ['fraud_data','assault_data','burglary_data','larceny_data']