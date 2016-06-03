#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''期货数据导入器'''
# 从万德导入期货数据到数据仓库
#
# Created by Shengying Pan, 2016
import dateutil.parser
import asyncio
import aiohttp
import json

from datetime import datetime
from datetime import timedelta

from importererrors import WindError
from importererrors import ServerError
from windutilities import get_instrument_info
from windutilities import get_future_quotes
from serverutilities import get_classification
from serverutilities import create_classification_for_instrument
from serverutilities import get_last_day

#helper function
def pad_month(month):
    '''this function makes sure a month string always has length of 2'''
    if month < 10:
        return '0' + str(month)
    else:
        return str(month)

def search_beg(symbol):
    if symbol == 'RB': 
        return '0909'
    elif symbol == 'HC': 
        return '1407'
    elif symbol == 'I': 
        return '1403'
    elif symbol == 'J': 
        return '1109'
    elif symbol == 'JM': 
        return '1307'
    else: return '1312'

def get_each(symbol, beg_time, end_time, exchange): 
    beg_year = int(beg_time[0:2])
    end_year = int(end_time[0:2])
    beg_mth = int(beg_time[2:4])
    end_mth = int(end_time[2:4])
    item = []
    for yr in range(beg_year, end_year+1, 1): 
        for mth in range(1,13,1):
            iid = pad_month(yr) + pad_month(mth)
            if iid >= beg_time and iid <= end_time: 
                item.append(iid)
    return item    

def fix_instrument(length, symbol, month, exchange): 
    '''this function fixes the format of instruments in exceptions id'''
    if int(month) < 1605:
        return 'TC' + month + '.' + exchange
    elif int(month) == 1605: 
        return symbol + month + '.' + exchange 
    else: 
        return symbol + month[4-length:] + '.' + exchange    

class FutureImporter:
    '''class to import future data'''
    def __init__(self, loop, wind, config):
        '''constructor'''
        self.loop = loop
        self.wind = wind
        self.config = config
        
    async def upload_item(self, item, end_date):
        '''this function upload the quotes of an instrument to server'''
        last_day = None
        try:
            last_day = await get_last_day('future', item, self.config)
        except ServerError as err:
            raise err

        # today = datetime.now().strftime('%Y-%m-%d')
        span_start = 'ED-365D'

        if last_day is not None:
            one_day = timedelta(days=1)
            start_day = last_day + one_day
            span_start = start_day.strftime('%Y-%m-%d')

        data = None
        try:
            data = await get_future_quotes(item, span_start, end_date, self.wind)
        except WindError as err:
            raise err

        with aiohttp.ClientSession() as session:
            url = self.config['server']['urls']['base']
            url = url + self.config['server']['urls']['future'] + '/' + item
            params = {'jwt': self.config['user']['jwt']}
            headers = {'content-type': 'application/json'}
            payload = {
                'wind': data
            }
            response = None
            result = None
            status = None
            try:
                response = await session.post(url, params=params, \
                    headers=headers, data=json.dumps(payload))
                result = await response.json()
                status = response.status
            except:
                raise ServerError(-1, '无法连接数据库服务')
            finally:
                if response is not None:
                    response.release()

            if status > 200:
                message = '合约' + item + '数据上传失败，错误：' + \
                    json.dumps(result)
                raise ServerError(status, message)
            else:
                print('合约' + item + '数据上传完成，服务器返回' + json.dumps(result))
                return True

    async def process_item(self, item):
        '''process a single item, everything is here'''
        print('开始处理合约 ' + item)
        info = None
        try:
            info = await get_instrument_info(item, self.wind)
            print(item, info)
        except WindError as err:
            print('万德错误，合约' + item + '错误 ' + str(err))
            return False
        if info is None:
            print('万德错误，合约' + item + '不存在')
            return False
        else: 
            if info['last_traded_day'] > info['last_trading_day']:
                #cannot trade, just ignore and move on
                print('合约' + item + '已停止交易') 
                
                classification = None
                try:
                    classification = await get_classification(item, self.config)
                except ServerError as err:
                    print('访问合约' + item + '分类信息失败：' + str(err))
                    return False

                if classification is not None: 
                    print('过期合约' + item + '已存在于数据库中，停止重复上传')
                    return False
                    #try:
                        #return await self.upload_item(item, classification['expiry'][0:10]) 
                    #except ServerError as err:
                        #print('合约' + item + '期货价格上传失败：' + str(err))
                        #return False
                    #except WindError as err:
                        #print('合约' + item + '获取万德数据失败：' + str(err))
                        #return False
                else: # classification DNE
                    try:
                        classification = await create_classification_for_instrument(item, info, self.config)
                    except ServerError as err:
                        print('创建合约' + item + '分类信息失败：' + str(err))
                        return False

                    try:
                        return await self.upload_item(item, classification['expiry'][0:10]) 
                    except ServerError as err:
                        print('合约' + item + '期货价格上传失败：' + str(err))
                        return False
                    except WindError as err:
                        print('合约' + item + '获取万德数据失败：' + str(err))
                        return False
            else: 
                today = datetime.now().strftime('%Y-%m-%d')
                classification = None
                try:
                    classification = await get_classification(item, self.config)
                except ServerError as err:
                    print('访问合约' + item + '分类信息失败：' + str(err))
                    return False 
                if classification is None: 
                    try:
                        classification = await create_classification_for_instrument(item, info, self.config)
                    except ServerError as err:
                        print('创建合约' + item + '分类信息失败：' + str(err))
                        return False
                try:
                    return await self.upload_item(item, today)
                except ServerError as err:
                    print('合约' + item + '期货价格上传失败：' + str(err))
                    return False
                except WindError as err:
                    print('合约' + item + '获取万德数据失败：' + str(err))
                    return False                    

    def contract_name(instrument, today, exchange, exception, in_exceptions): 
        items = [] 
        beg_time = search_beg(instrument)
        end_time = str(today.year + 1)[2:] + str(pad_month(today.month))
        for i in get_each(instrument, beg_time, end_time, exchange):
            if is_exception:
                iid = fix_instrument(exceptions[instrument], instrument, i, exchange)
            else: 
                iid = instrument + i + '.' + exchange
            items.append(iid)
        tasks = []
        for item in items:
            tasks.append(asyncio.ensure_future(self.process_item(item)))
        try:
            self.loop.run_until_complete(asyncio.wait(tasks))
        except:
            print('循环产生异常：正在处理品种' + instrument + '期货价格数据')
        finally:
            print('完成品种 ' + instrument + ' 交易所 ' + instrument + '期货价格数据') 

    def run(self):
        '''main function to perform its duty'''
        print('处理期货合约价格数据')
        today = datetime.now()
        print('今天是', today.strftime('%Y-%m-%d'))
        
        exchanges = self.config['futures']['exchanges']
        exceptions = self.config['format']['exceptions']
        for exchange in dict.keys(exchanges):
            instruments = exchanges[exchange]
            for instrument in instruments:
                if instrument in exceptions: #'ZC' in 'CZC'
                    print('处理品种 ' + instrument + ' 交易所 ' + exchange)
                    contract_name(instrument, today, exchange, exceptions, True)                        
                else: # regular for 'SHF' and 'DCE'
                    print('处理品种 ' + instrument + ' 交易所 ' + exchange)
                    contract_name(instrument, today, exchange, exceptions, False)