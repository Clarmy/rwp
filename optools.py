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
import os
import pickle as pk
from datetime import datetime as dt
import time
from collections import defaultdict
import pdb


# def timing(func):
#     '''计时装饰器'''
#     def wrapper(*args, **kwargs):
#         # print('函数名:{0}'.format(func.__name__))
#         start_time = time.time()
#         result = func(*args, **kwargs)
#         end_time = time.time()
#         # print('用时{:.2f}秒'.format(end_time-start_time))
#         global time_statics
#         delt = end_time - start_time
#         time_statics[func.__name__] += delt

#         return result
#     return wrapper


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

    new_res_dict = defaultdict(set)
    for std_time in old_res_dict:
        unique_station = set([])
        for file_name in old_res_dict[std_time]:
            station_id = get_station_id(file_name)
            if station_id not in unique_station:
                unique_station.add(station_id)
                new_res_dict[std_time].add(file_name)
            else:
                pass

    return new_res_dict


def gather_res(file_name_lst, preset):
    '''收集文件源（文件名）'''
    res_dict = defaultdict(set)

    # （未处理）新集是当前全集减去前集
    queue = set(file_name_lst) - preset
    print('queue:{}'.format(len(queue)))
    # print('preset_files:{}'.format(len(preset)))

    # 对新集时间字符串进行匹配处理，匹配成功则添加到源字典中，匹配失败则忽略

    for file in queue:
        res_timestr = abstr_time(file, level='minute')
        # 每一个文件都在标准时间索引中找一个与之最近的时次，如果该结果与迭代中的时次相同，
        #  则可以追加到res_dict在该时次的列表里。
        match_timestr = match_standard(res_timestr,STD_INDEX)
        res_dict[match_timestr].add(file)
        preset.add(file)

    # 删除该时次重复的站
    res_dict = drop_duplicate_station(res_dict)

    # 删除到站不全的时次，待下次再次扫描
    to_remove = set([])
    for time_index in res_dict:
        if not is_station_enough(res_dict,time_index):
            for file in res_dict[time_index]:
                preset.remove(file)
            to_remove.add(time_index)

    # 在遍历过程中不能改变字典长度，因此在遍历结束以后删除要素
    try:
        for remove in to_remove:
            res_dict.pop(remove)
    except UnboundLocalError:
        pass

    newset = set(res_dict.keys())

    # 若新集存在，启动任务标识
    if newset:
        has_new_task = True
    else:
        has_new_task = False

    return res_dict, preset, queue, has_new_task


def is_station_enough(res_pool, timestr, threshold=70):
    '''判断文件源池中指定时次的站点数是否足够'''
    if len(res_pool[timestr]) < threshold:
        result = False
    else:
        result = True

    return result


def received_num(res_pool, timestr, threshold=70):
    '''返回已接收的站点数'''
    return len(res_pool[timestr])


def init_preset(path):
    '''初始化前集'''
    if not os.path.exists(path):
        with open(path, 'wb') as file_obj:
            pk.dump(set([]), file_obj)


def save_preset(preset, path):
    '''存储前集'''
    with open(path, 'wb') as file_obj:
        pk.dump(preset, file_obj)


def load_preset(path):
    '''加载前集'''
    with open(path, 'rb') as file_obj:
        try:
            preset = pk.load(file_obj)
        except EOFError:
            preset = set([])

    return preset


def standard_time_index():
    '''建立逐6分钟标准时间索引'''
    now = dt.utcnow()
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


def match_standard(timestr,STD_INDEX):
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

    delt = set([])
    for index, time_str in enumerate(STD_INDEX):
        if ymdh == time_str[:10]:
            delt.add((abs(minute-int(time_str[10:])), index))

    min_index = min(delt)[1]

    return STD_INDEX[min_index]


STD_INDEX = standard_time_index()


def main():
    '''主函数'''
    pass


if __name__ == '__main__':
    main()
