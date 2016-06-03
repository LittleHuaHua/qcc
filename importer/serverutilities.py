#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''Utilities helper for server connection'''
#
# Shengying Pan 2016-04-01
import asyncio
import aiohttp
import json
from datetime import datetime
from importererrors import ServerError
import dateutil.parser

async def create_classification_for_index(keyword, asset, index, options):
    '''this function creates a classification for the given index'''
    index_name = index['specs']
    with aiohttp.ClientSession() as session:
        url = options['server']['urls']['base']
        
        url = url + options['server']['urls']['classifications']
        params = {
            'jwt': options['user']['jwt']
        }
        headers = {
            'content-type': 'application/json'
        }
        payload = {
            'dataName': index['wind_id'],
            'displayName': index_name,
            'description': index_name,
            'keyword': keyword,
            'unit': index['unit'],
            'sector': index['sector'],
            'assetGroup': index['asset_group'],
            'asset': asset,
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
        except Exception as err:
            print(str(err))
            raise ServerError(-1, '无法连接数据库服务')
        finally:
            if response is not None:
                response.release()
        if status > 201:
            message = '指数' + index_name + '创建数据库分类失败，错误：' + \
                json.dumps(result)
            raise ServerError(status, message)
        else:
            print('指数' + index_name + '创建分类信息成功')
            return result

async def create_classification_for_product(keyword, asset, product, options):
    '''this function creates a classification for the given product'''
    product_name = product['origin'] + ' ' + product['specs']
    with aiohttp.ClientSession() as session:
        url = options['server']['urls']['base']
        url = url + options['server']['urls']['classifications']
        params = {
            'jwt': options['user']['jwt']
        }
        headers = {
            'content-type': 'application/json'
        }
        payload = {
            'dataName': product['wind_id'],
            'displayName': product_name,
            'description': product_name,
            'keyword': keyword,
            'unit': product['unit'],
            'sector': product['sector'],
            'assetGroup': product['asset_group'],
            'asset': asset,
            'subCategory3Value': product['origin'],
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
        except Exception as err:
            print(str(err))
            raise ServerError(-1, '无法连接数据库服务')
        finally:
            if response is not None:
                response.release()
        if status > 201:
            message = '产品' + product_name + '创建数据库分类失败，错误：' + \
                json.dumps(result)
            raise ServerError(status, message)
        else:
            print('产品' + product_name + '创建分类信息成功')
            return result

async def create_classification_for_volume(keyword, asset, volume, options):
    '''this function creates a classification for the given volume'''
    volume_name = volume['origin'] + ' ' + volume['specs']
    with aiohttp.ClientSession() as session:
        url = options['server']['urls']['base']
        url = url + options['server']['urls']['classifications']
        params = {
            'jwt': options['user']['jwt']
        }
        headers = {
            'content-type': 'application/json'
        }
        payload = {
            'dataName': volume['wind_id'],
            'displayName': volume_name,
            'description': volume_name,
            'keyword': keyword,
            'sector': volume['sector'],
            'assetGroup': volume['asset_group'],
            'asset':asset,
            'subCategory2Value': volume['category'],
            'subCategory3Value': volume['origin'],
            'unit': volume['unit'],
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
        except Exception as err:
            print(str(err))
            raise ServerError(-1, '无法连接数据库服务')
        finally:
            if response is not None:
                response.release()
        if status > 201:
            message = '产品' + volume_name + '创建数据库分类失败，错误：' + \
                json.dumps(result)
            raise ServerError(status, message)
        else:
            print('产品' + volume_name + '创建分类信息成功')
            return result

async def get_last_day(data_type, data_name, config):
    '''get last day for some data on server'''
    with aiohttp.ClientSession() as session:
        url = config['server']['urls']['base']
        url = url + config['server']['urls'][data_type] + '/' + data_name
        params = {
            'jwt': config['user']['jwt'],
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

        if status > 200:
            message = '数据' + data_type + data_name + '获取最后数据日期失败，错误：' + \
                json.dumps(result)
            raise ServerError(status, message)
        elif len(result) == 0:
            return None
        else:
            return dateutil.parser.parse(result[0]['date'])

async def create_classification_for_instrument(instrument, info, options):
    '''this function creates a classification for the given item'''
    with aiohttp.ClientSession() as session:
        url = options['server']['urls']['base']
        url = url + options['server']['urls']['classifications']
        params = {
            'jwt': options['user']['jwt']
        }
        headers = {
            'content-type': 'application/json'
        }
        payload = {
            'dataName': instrument,
            'displayName': instrument + '日价格',
            'description': info['asset'] + instrument + '价格',
            'keyword': '国内期货日价格',
            'unit': info['unit'],
            'asset': info['asset'],
            'expiry': info['last_trading_day'].strftime('%Y-%m-%d'),
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
        except Exception as err:
            print(str(err))
            raise ServerError(-1, '无法连接数据库服务')
        finally:
            if response is not None:
                response.release()

        if status > 201:
            message = '合约' + instrument + '创建数据库分类失败，错误：' + \
                json.dumps(result)
            raise ServerError(status, message)
        else:
            print('合约' + instrument + '创建分类信息成功')
            return result

async def get_classification(item, options):
    '''try to fetch the corresponding classification for an item on the server'''
    with aiohttp.ClientSession() as session:
        url = options['server']['urls']['base']
        url = url + options['server']['urls']['classifications']
        params = {
            'jwt': options['user']['jwt'],
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