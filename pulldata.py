import aiohttp
import orjson
import cython
import asyncio
import requests
import numpy as np
from sortedcontainers import SortedDict

async def connect_bitmex():
    bitmex_url = r"wss://www.bitmex.com/realtime"
    session = aiohttp.ClientSession(json_serialize=orjson.dumps)
    ws = await session.ws_connect(url=bitmex_url)
    connect_msg = await ws.receive_json(loads=orjson.loads)
    del(connect_msg)
    return ws

async def get_data(ws, command):
    await ws.send_json(command)
    connect_msg = await ws.receive_json(loads=orjson.loads)
    resp = await ws.receive_json(loads=orjson.loads)
    del(connect_msg)
    return resp

async def get_funding(ws):
    command = {
        "op": "subscribe",
        "args": "funding:XBTUSD"
    }
    resp = await get_data(ws, command)
    return resp["data"][0]["fundingRate"]

async def get_price(ws):
    command = {
        "op": "subscribe",
        "args": "quote:XBTUSD"
    }
    resp = await get_data(ws, command)
    return resp["data"][0]["askPrice"]

async def get_oi():
    oi_url = r"https://www.bitmex.com/api/v1/instrument?symbol=XBTUSD&count=1&reverse=true"
    resp = orjson.loads(requests.get(oi_url).text)
    return resp[0]["openInterest"]

async def get_vol():
    vol_url = r"https://www.bitmex.com/api/v1/instrument?symbol=XBTUSD&count=1&reverse=true"
    resp = orjson.loads(requests.get(vol_url).text)
    return resp[0]["volume"]

async def get_time():
    timestamp_url = r"https://www.bitmex.com/api/v1/instrument?symbol=XBTUSD&count=1&reverse=true"
    resp = orjson.loads(requests.get(timestamp_url).text)
    return resp[0]["timestamp"]

async def db_data():
    ws = await connect_bitmex()
    funding = await get_funding(ws)
    oi = await get_oi()
    vol = await get_vol()
    price = await get_price(ws)
    time = await get_time()


    info = {
        "time": time,
        "funding": funding,
        "oi" : oi,
        "vol" : vol,
        "price" : price
    }

    await ws.close()
    return info
