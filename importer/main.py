#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''entry point for wind data import'''
#
# Shengying Pan 2016-04-01
from WindPy import w
import json
import asyncio

from futureimporter import FutureImporter
# from spotimporter import SpotImporter
# from indeximporter import IndexImporter
# from volumeimporter import VolumeImporter

if __name__ == '__main__':
    '''main function'''
    print('开始程序')
    config = None
    with open('config.json', encoding='utf-8') as config_file:
        config = json.load(config_file, encoding='utf-8')
    if config is None:
        print('无法读取配置文件config.json')

    #move on to perform the actual job
    w.start(60)
    if not w.isconnected():
        print('无法连接万德')
    else:
        print('万德已连接')
        print('开始处理数据')
        #grab the main loop
        loop = asyncio.get_event_loop()
        #future quotes upload

        future_importer = FutureImporter(loop, w, config)
        future_importer.run()

        # spot_importer = SpotImporter(loop, w, config)
        # spot_importer.run()

        # index_importer = IndexImporter(loop, w, config)
        # index_importer.run()

        # volume_importer = VolumeImporter(loop, w, config)
        # volume_importer.run()

        loop.close()
    print('程序结束')
    w.stop()