from websocket import create_connection
import gzip
import time
import json
import csv

import pymongo
from pipelines import MongoDBPipeline


global globalTime
# 获取历史数据起始时间
globalTime = 1512057600
#1506787200



def loop_data(o, k=''):
    global json_ob, c_line
    if isinstance(o, dict):
        for key, value in o.items():
            if (k == ''):
                loop_data(value, key)
            else:
                loop_data(value, k + '.' + key)
    elif isinstance(o, list):
        for ov in o:
            loop_data(ov, k)
    else:
        if not k in json_ob:
            json_ob[k] = {}
        json_ob[k][c_line] = o





def write_csv(title, rows, csv_file_name):
    with open(csv_file_name, 'a+', newline='') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=title)
        writer.writerows(rows)


def json_to_csv(object_list):
    global json_ob, c_line
    json_ob = {}
    c_line = 0
    for ov in object_list:
        loop_data(ov)
        c_line += 1
    title, rows = get_title_rows(json_ob)


global count
count = 0
global x
x = globalTime
interval = 18000
if __name__ == '__main__':
    server = '192.168.0.103'
    port = 27017
    db = "huobi"
    name = ''
    passwd = ''
    col = '1day'    
    
    while (1):
        try:
            ws = create_connection("wss://api.huobi.br.com/ws")
            # ws = create_connection("wss://api.huobipro.com/ws")

            while (1):
                
                tradeStr = """{"req": "market.zecusdt.kline.1day","id": "id10", "from": """ + str(
                    globalTime) + """, "to":""" + str(1533536947) + """ }"""
                ws.send(tradeStr)
                compressData = ws.recv()
                if compressData != '':
                    result = gzip.decompress(compressData).decode('utf-8')
                    f=0
                else:
                    print("正在重新连接")
                    ws.connect("wss://api.huobi.br.com/ws")
                    # ws.connect("wss://api.huobipro.com/ws")

                    globalTime = x + (interval * (count + 1))
                    tradeStr2 = """{"req": "market.zecusdt.kline.1day","id": "id10", "from": """ + str(
                        globalTime) + """, "to":""" + str((globalTime + interval)) + """ }"""
                    ws.send(tradeStr2)
                    compressData = ws.recv()
                    result = gzip.decompress(compressData).decode('utf-8')

                if result[:7] == '{"ping"':
                    ts = result[8:21]
                    pong = '{"pong":' + ts + '}'
                    ws.send(pong)
                else:
                    resutlJson = json.loads(result)
                    data = resutlJson['data']
                    
                    
                    #json_to_csv(data)
                    count += 1
                    
                    print(data)
                globalTime += interval
        except:
            print('connect ws error,retry...')
            break
