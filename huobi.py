from websocket import create_connection
import gzip
import time
import json
import csv
import datetime
import pymongo
from pipelines import MongoDBPipeline


def write_csv(title, rows, csv_file_name):
    with open(csv_file_name, 'a+', newline='') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=title)
        writer.writerows(rows)


def json_to_mongo(conn,object_list,syb,period):
    
    
    
   
    for ov in object_list:
        temp_id=ov.pop('id')
        ov['_id'] = syb+"-"+str(temp_id)
        ov['date']=datetime.datetime.utcfromtimestamp(temp_id)      
        ov['symbol']=syb
        ov['period']=period
        conn.process_item(ov,col)

def ws_reconnection(ws,tradeStr):
    
           
    ws.send(tradeStr)
    compressData = ws.recv()
    if compressData != '':
        result = gzip.decompress(compressData).decode('utf-8')
        if syb not in result:
            result = ws_reconnection(ws,tradeStr)
        print(tradeStr)
        print(result)

        f=0                      
    else:
        print("正在重新连接")
        ws = create_connection("wss://api.huobi.br.com/ws")
        result = ws_reconnection(ws,tradeStr)
        f=0
    
    return result


if __name__ == '__main__':
    
    server = '192.168.0.103'
    port = 27017
    db = "huobi"
    name = ''
    passwd = ''
    col = 'kline'    
    conn = MongoDBPipeline(server, port, db, name, passwd, col)    
    
    timestamp2 = datetime.datetime.now()
    currenttime = int(timestamp2.strftime("%s"))
    
    interval = 3600*24*365
    period="1day"
    syb_list=["btcusdt","bchusdt","ethusdt","etcusdt","ltcusdt","eosusdt","adausdt","xrpusdt","dashusdt","omgusdt","iotausdt","zecusdt","steemusdt","hb10usdt","ontusdt","paiusdt","iostusdt","btmusdt","ocnusdt","zilusdt","ruffusdt","socusdt","dtausdt","iostusdt","ctxcusdt","elfusdt","trxusdt","itcusdt","actusdt","vetusdt","neousdt","wiccusdt","nasusdt","qtumusdt","elausdt","btsusdt","thetausdt","smtusdt","letusdt","cvcusdt","cmtusdt","hcusdt","mdsusdt","bixusdt","storjusdt","sntusdt","xemusdt","gntusdt"]
    #syb_list=["actusdt"]
    
    
 
    try:
        ws = create_connection("wss://api.huobi.br.com/ws")
        # ws = create_connection("wss://api.huobipro.com/ws")
        for syb in syb_list:
            print(syb)
            globalTime = 1512057600
            x = globalTime    
            count = 0
            while (globalTime< currenttime):
                
                if globalTime+interval <= currenttime:
                    tradeStr = '{"req": "market.'+syb+'.kline.'+period+'","id": "id10908", "from": ' + str(
                    globalTime) + ', "to":' + str(globalTime+interval) + ' }'
                else:
                    tradeStr = '{"req": "market.'+syb+'.kline.'+period+'","id": "id10908", "from": ' + str(
                    globalTime) + ', "to":' + str(currenttime) + ' }'                        
                result = ws_reconnection(ws,tradeStr)
                
                if result[:7] == '{"ping"':
                    ts = result[8:21]
                    pong = '{"pong":' + ts + '}'
                    ws.send(pong)
                else:
                    resutlJson = json.loads(result)
                    data = resutlJson['data']
                    print(data)
                    
                    json_to_mongo(conn,data,syb,period)
                    count += 1
                    globalTime += interval
                    
            
                
            
                    
    except Exception as e:
        print(e)
        #break
