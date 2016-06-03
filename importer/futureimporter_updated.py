import asyncio
import json

from datetime import datetime

#helper function
def pad_fn(num):
    '''this function makes sure a month string always has length of 2'''
    if num < 10:
        return '0' + str(num)
    else:
        return str(num)
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

def fix_instrument(length, symbol, month, exchange): 
    '''this function fixes the format of instruments in exceptions id'''
    if int(month) < 1605:
        return 'TC' + month + '.' + exchange
    elif int(month) == 1605: 
        return symbol + month + '.' + exchange 
    else: 
        return symbol + month[4-length:] + '.' + exchange    

def get_each(symbol, beg_time, end_time, exchange): 
    beg_year = int(beg_time[0:2])
    end_year = int(end_time[0:2])
    beg_mth = int(beg_time[2:4])
    end_mth = int(end_time[2:4])
    item = []
    for yr in range(beg_year, end_year+1, 1): 
        for mth in range(1,13,1):
            iid = pad_fn(yr) + pad_fn(mth)
            if iid >= beg_time and iid <= end_time: 
                item.append(iid)
    return item 

def run():
    '''main function to perform its duty'''
    print('处理期货合约价格数据')
    today = datetime.now()
    print('今天是', today.strftime('%Y-%m-%d'))
    with open('config.json', encoding='utf-8') as config_file: 
        config = json.load(config_file, encoding='utf-8')

    exchanges = config['futures']['exchanges']
    exceptions = config['format']['exceptions']
    for exchange in dict.keys(exchanges):
        instruments = exchanges[exchange]
        for instrument in instruments:
            if instrument in exceptions: #'ZC' in 'CZC'
                print('处理品种 ' + instrument + ' 交易所 ' + exchange)
                items = []
                beg_time = search_beg(instrument)
                end_time = str(today.year+1)[2:]  + str(pad_fn(today.month))
                for i in get_each(instrument, beg_time, end_time, exchange): 
                    iid = fix_instrument(exceptions[instrument], instrument, i, exchange)
                    items.append(iid)
                print(items)
            else: # regular for 'SHF' and 'DCE'
                print('处理品种 ' + instrument + ' 交易所 ' + exchange)
                items = []                    
                beg_time = search_beg(instrument)
                end_time = str(today.year+1)[2:] + str(pad_fn(today.month))
                for i in get_each(instrument, beg_time, end_time, exchange):
                    iid = instrument + i + '.' + exchange
                    items.append(iid)
                print(items)

run()