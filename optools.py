# coding : utf-8
'''
--------------------------------------------------------------------
项目名：WDPF ( WinD ProFile )
模块名：optools
该模块包含了本项目与业务化处理有关的函数
--------------------------------------------------------------------
python = 3.6
version = 0.0.1
--------------------------------------------------------------------
 李文韬   |   liwentao@mail.iap.ac.cn   |   https://github.com/Clarmy
--------------------------------------------------------------------
'''
from datetime import datetime as dt
import time


def timing(func):
    '''计时装饰器'''
    def wrapper(*args, **kwargs):
        print('函数名:{0}'.format(func.__name__))
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        print('用时{:.2f}秒'.format(end_time-start_time))
        return result
    return wrapper


def abstr_time(file_name, level='full'):
    '''从字符串提取时间'''
    if level == 'hour':
        result = file_name.split('_')[4][:10]
    elif level == 'minute':
        result = file_name.split('_')[4][:12]
    elif level == 'full':
        result = file_name.split('_')[4]

    return result


def drop_duplicate_station(old_res_dict):
    '''剔除标准时间轴下重复站点的文件源'''
    from collections import defaultdict

    def get_station_id(file_name):
        '''根据文件名提取站点号'''
        station_id = file_name.split('_')[3]
        return station_id

    new_res_dict = defaultdict(list)
    for std_time in old_res_dict:
        unique_station = set([])
        for file_name in old_res_dict[std_time]:
            station_id = get_station_id(file_name)
            if station_id not in unique_station:
                unique_station.add(station_id)
                new_res_dict[std_time].append(file_name)
            else:
                pass

    return new_res_dict


def gather_res(file_name_lst):
    '''收集文件源（文件名）'''
    std_index = standard_time_index()
    res_dict = {sti: [] for sti in std_index}
    for key in res_dict:
        for file_name in file_name_lst:
            res_timestr = abstr_time(file_name, level='minute')
            match_timestr = match_standard(res_timestr)
            if match_timestr == key:
                res_dict[key].append(file_name)

    return res_dict


def standard_time_index():
    '''建立逐6分钟标准时间索引'''
    now = dt.now()
    year = str(now.year)
    month = str(now.month).zfill(2)
    day = str(now.day).zfill(2)
    date = ''.join([year, month, day])
    hours = [str(n).zfill(2) for n in range(24)]
    minutes = [str(n).zfill(2) for n in range(0, 60, 6)]

    # stdt : standard time
    stdt_index = []
    for hour in hours:
        for minute in minutes:
            stdt_index.append(''.join([date, hour, minute]))

    return tuple(stdt_index)


def match_standard(timestr):
    '''（规定格式的）任意时间字符串向标准时间索引的匹配

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

    # ymdh : year-month-day-hour
    ymdh = timestr[:10]
    minute = int(timestr[10:])

    sdt_index = standard_time_index()
    delt = []
    for index, time_str in enumerate(sdt_index):
        if ymdh == time_str[:10]:
            delt.append((abs(minute-int(time_str[10:])), index))

    min_index = min(delt)[1]

    return sdt_index[min_index]


def main():
    '''主函数'''
    pass


if __name__ == '__main__':
    main()
