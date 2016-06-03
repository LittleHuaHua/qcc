#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''指数数据导入器'''
# 从万德导入指数数据到数据仓库
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
from serverutilities import create_classification_for_index
from serverutilities import get_last_day
from windutilities import get_index_quotes

class IndexImporter:
    '''class to import spot data'''
    def __init__(self, loop, wind, config):
        '''constructor'''
        self.loop = loop
        self.wind = wind
        self.config = config
    def run(self):
        '''main function to import data'''
        print('处理产品指数价格')
        today = datetime.now()
        print('今天是', today.strftime('%Y-%m-%d'))
        assets = self.config['indexes']['国外指数日价格']
        for asset in dict.keys(assets):
            print('处理品种' + asset)
            items = assets[asset]
            tasks = []
            for item in items:
                tasks.append(asyncio.ensure_future(self.process_item('国外指数日价格', asset, item)))
            try:
                self.loop.run_until_complete(asyncio.wait(tasks))
            except:
                print('循环产生异常：正在处理产品' + asset + '国外指数价格数据')

    async def upload_item(self, asset, item):
        '''this function upload the spot quotes of a product'''
        item_name = asset + item['specs']
        last_day = None
        try:
            last_day = await get_last_day('index', item['wind_id'], self.config)
        except ServerError as err:
            raise err


        today = datetime.now().strftime('%Y-%m-%d')
        start_date = self.config['indexes']['start']

        if last_day is not None:
            one_day = timedelta(days=1)
            start_date = (last_day + one_day).strftime('%Y-%m-%d')

        data = None
        try:
            data = await get_index_quotes(item['wind_id'], start_date, today, self.wind)
        except WindError as err:
            raise err

        with aiohttp.ClientSession() as session:
            url = self.config['server']['urls']['base']
            url = url + self.config['server']['urls']['index'] + '/' + item['wind_id']
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
                message = '产品' + item_name + '数据上传失败，错误：' + \
                    json.dumps(result)
                raise ServerError(status, message)
            else:
                print('产品' + item_name + '数据上传完成，服务器返回' + json.dumps(result))
                return True


    async def process_item(self, keyword, asset, item):
        '''process a single item'''
        item_name = asset + item['specs']
        print('开始处理产品' + item_name)
        classification = None
        try:
            classification = await get_classification(item['wind_id'], self.config)
        except ServerError as err:
            print('访问产品' + item_name + '分类信息失败：' + str(err))
            return False

        if classification is None:
            try:
                classification = await create_classification_for_index(keyword, asset, item, self.config)
            except ServerError as err:
                print('创建产品' + item_name + '分类信息失败：' + str(err))
                return False

        try:
            return await self.upload_item(asset, item)
        except ServerError as err:
            print('产品' + item_name + '现货价格上传失败: ' + str(err))
            return False
        except WindError as err:
            print('产品' + item_name + '获取万德数据失败：' + str(err))
            return False

        return True