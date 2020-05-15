from pulldata import db_data
import asyncio
import time
from pymongo import MongoClient

def update_db():
    #mongodb info
    connection_string = 'mongodb+srv://bitmex-trainer:oFY0mQGJTw4UMdjJ@bitmex-training-mqvgf.gcp.mongodb.net/test?retryWrites=true&w=majority'
    client = MongoClient(connection_string)
    db = client.bitmex
    training_data = db.training_data

    while True:
        #get current data to put into db
        current_data = asyncio.run(db_data())
        #get most recent price to see if its a buy/sell
        prev_price = db.training_data.find_one(sort=[( '_id', -1)]).get("price")
        result = current_data.get("result")
        if prev_price > current_data.get("price"):
            result = -1
        elif prev_price < current_data.get("price"):
            result = 1
        current_data.update({'result' : result})
        print(current_data)
        #insert into the db
        training_data.insert_one(current_data)
        time.sleep(300)

asyncio.run(update_db())
