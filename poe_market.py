# -*- coding: utf-8 -*-
"""
Created on Thu Aug 22 22:07:18 2019

@author: Rod
"""
import json
import requests
import pandas as pd
import math
import re
import numpy as np
league_name="凋落聯盟"
# =============================================================================
# 
# =============================================================================
from google.cloud import bigquery as bq
import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="./dashboardmap-800d89b22555.json"
client = bq.Client()
print(client)



dataset_id = f"{client.project}.NEW_DATA_SET"
table_id = f"{dataset_id}.TEST_TABLE"
tschema = [
bq.SchemaField("full_name", "STRING", mode="NULLABLE"),
bq.SchemaField("age", "INTEGER", mode="NULLABLE"),
]
dataset = bq.Dataset(dataset_id)
dataset.location = "asia-east1"
dataset = client.create_dataset(dataset_id)
table = bq.Table(table_id, schema=tschema)
table = client.create_table(table)  # API request
print(f"Created dataset {client.project}.{dataset.dataset_id}")
print(f"Created table {table.project}.{table.dataset_id}.{table.table_id}")




# =============================================================================
# currency
# =============================================================================


#query curr, use F12 to find query
query_curr1 = {"exchange":{"status":{"option":"online"},"have":["chaos"],"want":["exa"]}}
#convert dic to json str
query_curr1=json.dumps(query_curr1)
query_curr1_ID = requests.get(url="http://web.poe.garena.tw/api/trade/exchange/"+league_name+"?source=" +(query_curr1))
#query_curr1_ID = requests.post(url="http://web.poe.garena.tw/api/trade/exchange/%E6%88%B0%E4%BA%82%E8%81%AF%E7%9B%9F", data= query_curr1)
query_curr1_list = json.loads(query_curr1_ID.text)
#get the number
stop_num = math.ceil(query_curr1_list['total']/10) 

query_curr1_all = []
for i in range(0,stop_num):
    query_curr1_str = ','.join(query_curr1_list['result'][i*10:(i+1)*10])
    query_curr1_detail = requests.get("http://web.poe.garena.tw/api/trade/fetch" +"/"+ query_curr1_str +"?query=elJEbsL&exchange").json()
#    print(i)
    query_curr1_all.extend(query_curr1_detail['result'])

for i in range(len(query_curr1_all)):
    query_curr1_all[i]['listing']['price']['exchange']['amount']
# RE找出數字 → list → str → int
#curr_list = [','.join(re.findall(r"(\d+)",query_curr1_all[i]['item']['note'])) for i in range(len(query_curr1_all))]
#找出有多少各ID數量可交易
curr_list = [query_curr1_all[i]['listing']['price']['exchange']['amount'] \
             for i in range(len(query_curr1_all))]
# int 2 str, replace ',' to '.', str 2 float, float 2 round2
curr_list = [round(float(str(w).replace(',', '.')),2) for w in curr_list]

for i in range(len(query_curr1_all)):
    query_curr1_all[i]['listing']['price']['item']['stock']

curr_list = [query_curr1_all[i]['listing']['price']['item']['stock'] \
             for i in range(len(query_curr1_all))]


exa2c = np.median(curr_list)
np.mean(curr_list)


# =============================================================================
# item
# =============================================================================

#query 
query_item1 = {"query":{"status":{"option":"online"},"name":"薛朗的護身長袍","type":"秘術長衣","stats":[{"type":"and","filters":[],"disabled":False}]},"sort":{"price":"asc"}}
#dic to json str
query_item1=json.dumps(query_item1)


#query all ID 
all_data = requests.get(url="http://web.poe.garena.tw/api/trade/search/"+league_name+"?source=" +(query_item1))
hunt_list = json.loads(all_data.text)


#批量查詢詳細內容
stop_num = math.ceil(hunt_list['total']/10) 
detail_data_all = []
for i in range(0,stop_num):
    hunt_list_str = ','.join(hunt_list['result'][i*10:(i+1)*10])
    detail_data = requests.get("http://web.poe.garena.tw/api/trade/fetch" +"/"+ hunt_list_str +"?query=q9Wlyktg").json()
    detail_data_all.extend(detail_data['result'])


