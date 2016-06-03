#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''Utilities helper for WindPy'''
#
# Shengying Pan 2016-04-01
import math
import asyncio
import aiohttp
from datetime import datetime
from importererrors import WindError

#helper
def fix_number(number):
    '''this function converts NaN to None'''
    if number is None:
        return None
    elif math.isnan(number):
        return None
    else:
        return number

async def get_future_quotes(instrument, span, end, wind):
    '''retrieves future quotes from wind'''
    qry = 'open,high,low,close,volume,oi'
    result = wind.wsd(instrument, qry, span, end, '')
    if result.ErrorCode != 0:
        raise WindError(result.ErrorCode, result.Data[0][0])
    else:
        print('合约' + instrument + '万德数据获得完毕，范围' + span + '至' + end)
        data_list = preprocess_future_data(result)
        return data_list

async def get_spot_quotes(wid, start_date, end_date, wind):
    '''retrive spot quotes from wind'''
    result = wind.edb(wid, start_date, end_date, '')
    if result.ErrorCode != 0:
        raise WindError(result.ErrorCode, result.Data[0][0])
    else:
        print('产品' + wid + '万德数据获得完毕，范围' + start_date + '至' + end_date)
        data_list = []
        for i in range(0, len(result.Times)):
            date = result.Times[i].strftime('%Y-%m-%d')
            price = fix_number(result.Data[0][i])
            if price is None:
                continue
            else:
                data_item = [date, price]
                data_list.append(data_item)
        return data_list

async def get_index_quotes(wid, start_date, end_date, wind):
    '''retrive index quotes from wind'''
    result = wind.edb(wid, start_date, end_date, '')
    if result.ErrorCode != 0:
        raise WindError(result.ErrorCode, result.Data[0][0])
    else:
        print('产品' + wid + '万德数据获得完毕，范围' + start_date + '至' + end_date)
        data_list = []
        for i in range(0, len(result.Times)):
            date = result.Times[i].strftime('%Y-%m-%d')
            price = fix_number(result.Data[0][i])
            if price is None:
                continue
            else:
                data_item = [date, price]
                data_list.append(data_item)
        return data_list

async def get_volume_quotes(wid, start_date, end_date, wind):
    '''retrive volume quotes from wind'''
    result = wind.edb(wid, start_date, end_date, '')
    if result.ErrorCode != 0:
        raise WindError(result.ErrorCode, result.Data[0][0])
    else:
        print('产品' + wid + '万德数据获得完毕，范围' + start_date + '至' + end_date)
        data_list = []
        for i in range(0, len(result.Times)):
            date = result.Times[i].strftime('%Y-%m-%d')
            price = fix_number(result.Data[0][i])
            if price is None:
                continue
            else:
                data_item = [date, price]
                data_list.append(data_item)
        return data_list

async def get_instrument_info(instrument, wind):
    '''retrieves info for an instrument from wind'''
    qry = 'lasttrade_date,last_trade_day,contractmultiplier,punit,sccode'
    today = datetime.now().strftime('%Y-%m-%d')
    result = wind.wsd(instrument, qry, 'ED0D', today, 'Days=Alldays')
    if result.ErrorCode != 0:
        raise WindError(result.ErrorCode, result.Data[0][0])
    elif result.Data[0][0] is not None:
        #print('获取万德信息，合约' + instrument)
        #print(result)
        info = {
            'last_trading_day': result.Data[0][0],
            'last_traded_day': result.Data[1][0],
            'unit': str(result.Data[2][0]) + result.Data[3][0],
            'asset': result.Data[4][0]
        }
        return info
    else:
        return None

def preprocess_future_data(data):
    '''this function converts wind data to csv compatible format'''
    '''it also removes all empty rows'''
    data_list = []
    for i in range(0, len(data.Times)):
        date = data.Times[i].strftime('%Y-%m-%d')
        open_price = fix_number(data.Data[0][i])
        highest_price = fix_number(data.Data[1][i])
        lowest_price = fix_number(data.Data[2][i])
        close_price = fix_number(data.Data[3][i])
        volume = fix_number(data.Data[4][i])
        open_interest = fix_number(data.Data[5][i])
        if open_price is None and highest_price is None and \
            lowest_price is None and close_price is None and \
            volume is None and open_interest is None:
            continue
        else:
            data_item = [date, open_price, highest_price, lowest_price, \
                close_price, volume, open_interest]
            data_list.append(data_item)
    return data_list