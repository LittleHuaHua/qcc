#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''Upload Data From Wind To DB'''
#
# Shengying Pan 2016-04-01
import math
import json
from datetime import datetime
from datetime import timedelta
import asyncio
import aiohttp
import dateutil.parser
from WindPy import w

#helpers begin
def pad_month(month):
    '''this function makes sure a month string always has length of 2'''
    if month < 10:
        return '0' + str(month)
    else:
        return str(month)

def fix_number(number):
    '''this function converts NaN to None'''
    if math.isnan(number):
        return None
    else:
        return number

def fix_result(result):
    '''this function fixes results'''
    if not isinstance(result, tuple):
        return '任务失败：' + result
    elif result[0] != None:
        return '任务失败：' + result[0]
    else:
        return '任务成功：' + result[1]
#helpers end



class Importer:
    '''class to import data from wind to db'''
    def __init__(self):
        '''constructor function'''
        with open('config.json') as config_file:
            self.config = json.load(config_file, encoding='utf-8')


    def fix_instrument(self, instrument, exchange):
        '''this function fixes the format of instrument id'''
        length = self.config['format']['instrument'][exchange]
        return instrument[4-length:]

    def start(self):
        '''the main function to perform all operations'''
        print('开始程序')
        w.start(60)
        #if cannot connect to wind, just stop
        if not w.isconnected():
            print('无法连接万德')
            print('程序结束')
            w.stop()
            return
        #otherwise
        print('万德已连接')
        print('开始处理数据')
        #期货行情

        today = datetime.now()
        print('今天是', today.strftime('%Y-%m-%d'))
        loop = asyncio.get_event_loop()
        exchanges = self.config['futures']['exchanges']
        for eid in dict.keys(exchanges):
            instruments = exchanges[eid]
            instrument = None
            for instrument in instruments:
                print('处理品种 ' + instrument + ' 交易所 ' + eid)
                items = []
                i = 0
                for i in range(today.month, today.month + 13, 1):
                    iid = instrument
                    if i > 12:
                        iid = iid + self.fix_instrument(str(today.year + 1)[2:] + \
                            pad_month(i % 12), eid)
                    else:
                        iid = iid + self.fix_instrument(str(today.year)[2:] + \
                            pad_month(i), eid)
                    iid = iid + '.' + eid
                    items.append(iid)

                tasks = []
                for item in items:
                    tasks.append(asyncio.ensure_future(self.process_item(w, item)))

                try:
                    loop.run_until_complete(asyncio.wait(tasks))
                except:
                    print('循环产生异常：处理品种' + instrument + '期货价格数据')
                finally:
                    print('完成品种 ' + instrument + ' 交易所 ' + eid)
        loop.close()
        print('程序结束')
        w.stop()


    async def process_item(self, wind, item):
        '''this function process a single future daily quote item'''
        #this function needs to be exception safe
        #a.k.a. not raising any exceptions
        print('开始处理合约 ' + item)
        #first check if instrument is still active
        try:
            info = await self.get_info(wind, item)
        except WindError as err:
            print('万德错误，合约' + item + '错误 ' + str(err))
            return False

        #now we have info
        if info is None:
            print('万德错误，合约' + item + '不存在')
            return False
        elif info['lastTradedDay'] > info['lastTradingDay']:
            #cannot trade, just ignore and move on
            print('万德错误，合约' + item + '已停止交易')
            return False

        #now move on to classification
        try:
            classification = await self.check_classification(item)
        except ServerError as err:
            print('访问合约' + item + '分类信息失败：' + str(err))
            return False

        if classification is None:
            try:
                classification = await self.create_classification(item, info)
            except ServerError as err:
                print('创建合约' + item + '分类信息失败：' + str(err))
                return False

        #now we have classification
        try:
            return await self.upload_future_item(wind, item)
        except ServerError as err:
            print('合约' + item + '期货价格上传失败：' + str(err))
            return False
        except WindError as err:
            print('合约' + item + '获取万德数据失败：' + str(err))
            return False


    async def get_info(self, wind, item):
        '''this function get instrument info from wind'''
        qry = 'lasttrade_date,last_trade_day,contractmultiplier,punit,sccode'
        today = datetime.now().strftime('%Y-%m-%d')
        result = wind.wsd(item, qry, 'ED0D', today, 'Days=Alldays')
        if result.ErrorCode != 0:
            raise WindError(result.ErrorCode, result.Data[0][0])
        elif result.Data[0][0] is not None:
            info = {
                'lastTradingDay': result.Data[0][0],
                'lastTradedDay': result.Data[1][0],
                'unit': str(result.Data[2][0]) + result.Data[3][0],
                'asset': result.Data[4][0]
            }
            return info
        else:
            return None

    async def check_classification(self, item):
        '''this function checks whether a classification for an item exists'''
        with aiohttp.ClientSession() as session:
            url = self.config['server']['urls']['base']
            url = url + self.config['server']['urls']['classifications']
            params = {
                'jwt': self.config['user']['jwt'],
                'where': '{dataName: "' + item + '"}'
            }
            response = None
            result = None
            status = None
            try:
                response = await session.get(url, params=params)
                result = await response.json()
                status = response.status
            except:
                raise ServerError(-1, '无法连接数据库服务')
            finally:
                if response is not None:
                    response.release()

            if status > 200:
                message = '合约' + item + '访问数据库分类失败，错误：' + \
                    json.dumps(result)
                raise ServerError(status, message)
            elif len(result) == 0:
                #need to create a classification
                return None
            else:
                return result[0]

    async def create_classification(self, item, info):
        '''this function creates a classification for the given item'''
        with aiohttp.ClientSession() as session:
            url = self.config['server']['urls']['base']
            url = url + self.config['server']['urls']['classifications']
            params = {
                'jwt': self.config['user']['jwt']
            }
            headers = {
                'content-type': 'application/json'
            }
            payload = {
                'dataName': item,
                'displayName': item,
                'description': info['asset'] + item + '价格',
                'keyword': '国内期货价格',
                'unit': info['unit'],
                'frequency': '日',
                'sector': info['sector'],
                'asset_group': info['asset_group'],
                'asset': info['asset'],
                'wind': True
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

            if status > 201:
                message = '合约' + item + '创建数据库分类失败，错误：' + \
                    json.dumps(result)
                raise ServerError(status, message)
            else:
                '合约' + item + '创建分类信息成功'
                return result




    async def get_last_day(self, item):
        '''this function get last day or None for an instrument from server'''
        with aiohttp.ClientSession() as session:
            url = self.config['server']['urls']['base']
            url = url + self.config['server']['urls']['future'] + '/' + item
            params = {
                'jwt': self.config['user']['jwt'],
                'order': 'date desc',
                'fields': 'date',
                'limit': '1'
            }
            response = None
            result = None
            status = None
            try:
                response = await session.get(url, params=params)
                result = await response.json()
                status = response.status
            except:
                raise ServerError(-1, '无法连接数据库服务')
            finally:
                if response is not None:
                    response.release()
            #success
            if status > 200:
                message = '合约' + item + '获取最后数据日期失败，错误：' + \
                    json.dumps(result)
                raise ServerError(status, message)
            elif len(result) == 0:
                return None
            else:
                return dateutil.parser.parse(result[0]['date'])


    def preprocess_wind_data(self, data):
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

    async def upload_future_item(self, wind, item):
        '''this function upload the quotes of an instrument to server'''
        last_day = None
        try:
            last_day = await self.get_last_day(item)
        except ServerError as err:
            raise err

        today = datetime.now().strftime('%Y-%m-%d')
        span_start = 'ED-365D'

        if last_day is not None:
            one_day = timedelta(days=1)
            start_day = last_day + one_day
            span_start = start_day.strftime('%Y-%m-%d')
        wind_result = wind.wsd(item, 'open,high,low,close,volume,oi', span_start, today, '')
        if wind_result.ErrorCode != 0:
            raise WindError(wind_result.ErrorCode, wind_result.Data[0][0])
        else:
            print('合约' + item + '万德数据获得完毕，范围' + span_start + '至' + today)
            data_list = self.preprocess_wind_data(wind_result)
            with aiohttp.ClientSession() as session:
                url = self.config['server']['urls']['base']
                url = url + self.config['server']['urls']['future'] + '/' + item
                params = {'jwt': self.config['user']['jwt']}
                headers = {'content-type': 'application/json'}
                payload = {
                    'wind': data_list
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