#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''数量数据导入器'''
# 从万德导入数量数据到数据仓库
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
from serverutilities import get_classification
from serverutilities import create_classification_for_volume
from serverutilities import get_last_day
from windutilities import get_volume_quotes


class VolumeImporter:
    '''class to import volume data'''

    def __init__(self, loop, wind, config):
        '''constructor'''
        self.loop = loop
        self.wind = wind
        self.config = config

    def run(self):
        '''main function to import data'''
        print('处理产品数量数据')
        today = datetime.now()

        print('今天是', today.strftime('%Y-%m-%d'))

        assets = self.config['volumes']['国内日数量']
        for asset in dict.keys(assets):
            print('处理品种' + asset)
            items = assets[asset]
            tasks = []
            for item in items:
                tasks.append(asyncio.ensure_future(
                    self.process_item('国内日数量', asset, item)))
            try:
                self.loop.run_until_complete(asyncio.wait(tasks))
            except:
                print('循环产生异常：正在处理产品' + asset + '国内日数量')

        assets = self.config['volumes']['国外日数量']
        for asset in dict.keys(assets):
            print('处理品种' + asset)
            items = assets[asset]
            tasks = []
            for item in items:
                tasks.append(asyncio.ensure_future(
                    self.process_item('国外日数量', asset, item)))
            try:
                self.loop.run_until_complete(asyncio.wait(tasks))
            except:
                print('循环产生异常：正在处理产品' + asset + '国外日数量')

        assets = self.config['volumes']['国内周数量']
        for asset in dict.keys(assets):
            print('处理品种' + asset)
            items = assets[asset]
            tasks = []
            for item in items:
                tasks.append(asyncio.ensure_future(
                    self.process_item('国内周数量', asset, item)))
            try:
                self.loop.run_until_complete(asyncio.wait(tasks))
            except:
                print('循环产生异常：正在处理产品' + asset + '国内周数量')

        assets = self.config['volumes']['国外周数量']
        for asset in dict.keys(assets):
            print('处理品种' + asset)
            items = assets[asset]
            tasks = []
            for item in items:
                tasks.append(asyncio.ensure_future(
                    self.process_item('国外周数量', asset, item)))
            try:
                self.loop.run_until_complete(asyncio.wait(tasks))
            except:
                print('循环产生异常：正在处理产品' + asset + '国外周数量')

        assets = self.config['volumes']['国内月数量']
        for asset in dict.keys(assets):
            print('处理品种' + asset)
            items = assets[asset]
            tasks = []

            for item in items:
                tasks.append(asyncio.ensure_future(
                    self.process_item('国内月数量', asset, item)))
            try:
                self.loop.run_until_complete(asyncio.wait(tasks))
            except:
                print('循环产生异常：正在处理产品' + asset + '国内月数量')

        assets = self.config['volumes']['国外月数量']
        for asset in dict.keys(assets):
            print('处理品种' + asset)
            items = assets[asset]
            tasks = []
            for item in items:
                tasks.append(asyncio.ensure_future(
                    self.process_item('国外月数量', asset, item)))
            try:
                self.loop.run_until_complete(asyncio.wait(tasks))
            except:
                print('循环产生异常：正在处理产品' + asset + '国外月数量')

    async def upload_item(self, keyword, asset, item):
        '''this function upload the volume quotes of a product'''
        item_name = asset + item['origin'] + item['specs']
        last_day = None
        if keyword == '国内日数量' or keyword == '国外日数量':
            vol = 'volume-daily'
        elif keyword == '国内周数量' or keyword == '国外周数量':
            vol = 'volume-weekly'
        elif keyword == '国内月数量' or keyword == '国外月数量':
            vol = 'volume-monthly'
        try:
            last_day = await get_last_day(vol, item['wind_id'], self.config)
        except ServerError as err:
            raise err

        today = datetime.now().strftime('%Y-%m-%d')
        start_date = self.config['volumes']['start']

        if last_day is not None:
            one_day = timedelta(days=1)
            start_date = (last_day + one_day).strftime('%Y-%m-%d')

        data = None
        try:
            data = await get_volume_quotes(item['wind_id'], start_date, today, self.wind)
        except WindError as err:
            raise err

        with aiohttp.ClientSession() as session:
            url = self.config['server']['urls']['base']
            if keyword == '国内日数量' or keyword == '国外日数量':
                url = url + \
                    self.config['server']['urls'][
                        'volume-daily'] + '/' + item['wind_id']
            elif keyword == '国内周数量' or keyword == '国外周数量':
                url = url + \
                    self.config['server']['urls'][
                        'volume-weekly'] + '/' + item['wind_id']
            elif keyword == '国内月数量' or keyword == '国外月数量':
                url = url + \
                    self.config['server']['urls'][
                        'volume-monthly'] + '/' + item['wind_id']

            params = {'jwt': self.config['user']['jwt']}
            headers = {'content-type': 'application/json'}
            payload = {
                'wind': data
            }
            response = None
            result = None
            status = None
            try:
                response = await session.post(url, params=params,
                                              headers=headers, data=json.dumps(payload))
                result = await response.json()
                status = response.status
            except:
                raise ServerError(-1, '无法连接数据库服务')
            finally:
                if response is not None:
                    response.release()

            if status > 200:
                message = '产品' + item_name + '数据上传失败，错误：' + \
                    json.dumps(result)
                raise ServerError(status, message)
            else:
                print('产品' + item_name + '数据上传完成，服务器返回' + json.dumps(result))
                return True

    async def process_item(self, keyword, asset, item):
        '''process a single item'''
        item_name = asset + item['specs']
        print('开始处理数据' + item_name)
        classification = None
        try:
            classification = await get_classification(item['wind_id'], self.config)
        except ServerError as err:
            print('访问数据' + item_name + '分类信息失败：' + str(err))
            return False

        if classification is None:
            try:
                classification = await create_classification_for_volume(keyword, asset, item, self.config)
            except ServerError as err:
                print('创建数据' + item_name + '分类信息失败：' + str(err))
                return False

        try:
            return await self.upload_item(keyword, asset, item)
        except ServerError as err:
            print('数据' + item_name + '数量价格上传失败: ' + str(err))
            return False
        except WindError as err:
            print('数据' + item_name + '获取万德数据失败：' + str(err))
            return False

        return True
