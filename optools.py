# coding : utf-8

import os
from datetime import datetime as dt
import time
import pdb

def timing(func):
    '''
    计时装饰器
    '''
    def wrapper(*args,**kwargs):
        print('函数名:{0}'.format(func.__name__))
        t0 = time.time()
        result = func(*args,**kwargs)
        t1 = time.time()
        print('用时{:.2f}秒'.format(t1-t0))
        return result
    return wrapper

@timing
def standardTimeIndex():
    '''
    建立逐6分钟标准时间索引
    '''
    now = dt.now()
    year = str(now.year)
    month = str(now.month).zfill(2)
    day = str(now.day).zfill(2)
    date = ''.join([year, month, day])
    hours = [str(n).zfill(2) for n in range(24)]
    minutes = [str(n).zfill(2) for n in range(0, 60, 6)]

    standard_time_index = []
    for h in hours:
        for m in minutes:
            standard_time_index.append(''.join([date, h, m]))

    return tuple(standard_time_index)

@timing
def matchStandard(timestr):
    '''
    （规定格式的）任意时间字符串向标准时间索引的匹配

    输入参数
    -------
    timestr : `string`
        时间字符串，其长度为12，精确到分钟，例如:201809071453

    返回值
    -----
    `string`
        经匹配最近的标准时间索引
    '''
    assert len(timestr) == 12

    ymdh = timestr[:10]
    minute = int(timestr[10:])

    sdt_index = standardTimeIndex()
    delt = []
    for n, i in enumerate(sdt_index):
        if ymdh == i[:10]:
            delt.append((abs(minute-int(i[10:])), n))

    min_index = min(delt)[1]

    return sdt_index[min_index]

@timing
def abstrTime(fn,res='WPRD',level='full'):
    if level == 'hour':
        return fn.split('_')[4][:10]
    elif level == 'minute':
        return fn.split('_')[4][:12]
    elif level == 'full':
        return fn.split('_')[4]

@timing
def gatherRes(fn_lst):
    std_index = standardTimeIndex()
    res_pool = {sti:[] for sti in std_index}
    for k in res_pool:
        for fn in fn_lst:
            res_timestr = abstrTime(fn,level='minute')
            match_timestr = matchStandard(res_timestr)
            if match_timestr == k:
                res_pool[k].append(fn)

    return res_pool



def main():
    pass


if __name__ == '__main__':
    main()
