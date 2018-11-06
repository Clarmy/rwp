# coding:utf-8
'''
--------------------------------------------------------------------
项目名：rwp
模块名：makegrid
本模块用于对不规则站点数据进行格点化插值处理
--------------------------------------------------------------------
python = 3.6
依赖库：
    netCDF4       $ conda install netCDF4
--------------------------------------------------------------------
'''

import sys
sys.path.append('..')
import json as js
import numpy as np
import netCDF4 as nc
from scipy.interpolate import griddata, interp1d
from algom.wprio import save_as_nc, load_js
import datetime
import ipdb


class OutputError(Exception):
    '''输出错误'''
    def __init__(self, message):
        self.message = message


def get_attr_dict():
    attr_dict = {'HWD':{'long_name':'Horizontal Wind Direction',
                        'units':'degree'},
                 'HWS':{'long_name':'Horizontal Wind Speed',
                        'units':'m/s'},
                 'VWS':{'long_name':'Vertical Wind Speed',
                        'units':'m/s'},
                 'HDR':{'long_name':'Horizontal Direction Reliability',
                        'units':'%'},
                 'VDR':{'long_name':'Vertical Direction Reliability',
                        'units':'%'},
                 'CN2':{'long_name':' index of refraction structure function',
                        'units':'None'},
                 'level':{'long_name':'Sampling Height level',
                        'units':'m'},
                 'lon':{'long_name':'longitudes','units':'degree_east'},
                 'lat':{'long_name':'latitudes','units':'degree_north'},
                 'time':{'long_name':'datetime',
                         'units':'minutes since 2018-01-01 00:00:00'}}
    return attr_dict


def std_sh():
    '''获取标准采样高度层'''
    sh1 = range(100,2000,100)
    sh2 = range(2000,5000,250)
    sh3 = range(5000,9500,500)
    sh = list(sh1)+list(sh2)+list(sh3)

    return sh


def v_interp(single_ds):
    '''垂直插值单站数据集

    输入参数
    -------
    single_ds : `dict`
        单站数据字典，包含有高度、站点信息及需插值变量等变量。

    返回值
    -----
    new_single_ds : `dict`
        经过垂直插值处理后的单站数据集，经处理后其高度层为统一结构（100~9000,40层），
        缺省值为np.nan。
    '''
    # 制作标准高度层
    sh = std_sh()

    raw_sh = single_ds['SH']

    # 获取上下边界索引
    raw_top = max(raw_sh)
    raw_bottom = min(raw_sh)

    for n, height in enumerate(sh):
        if height > raw_top:
            top_index = n
            break
    else:
        top_index = len(sh)

    for n, height in enumerate(sh):
        if height > raw_bottom:
            bottom_index = n
            break

    nsh = sh[bottom_index:top_index]
    headcount = len(sh[:bottom_index])
    tailcount = len(sh[top_index:])

    intp_vars = ['HWD', 'HWS', 'VWS', 'HDR', 'VDR', 'CN2']
    attr_vars = ['station', 'lon', 'lat', 'altitude', 'wave', 'time']

    new_single_ds = {}
    new_single_ds['SH'] = sh
    for av in attr_vars:
        new_single_ds[av] = single_ds[av]

    for var in intp_vars:
        intp_func = interp1d(raw_sh, single_ds[var], kind='slinear')
        new_single_ds[var] = [np.nan] * headcount + list(intp_func(nsh)) +\
            [np.nan] * tailcount

    return new_single_ds


def multi_v_interp(raw_dataset):
    '''多站（全数据集）垂直积分

    输入参数
    -------
    raw_dataset : `list`
        多站数据列表，单行是单站数据（字典格式）

    返回值
    -----
    `list`
        经插值处理后的多站数据列表
    '''
    return [v_interp(line) for line in raw_dataset]


def full_interp(pfn, varkeys=['HWD', 'HWS', 'VWS', 'HDR',
    'VDR', 'CN2'], method='cubic', attr=False, savepath=None):
    '''在单个站点垂直插值的基础上对所有站点所有层次进行插值处理

    输入参数
    -------
    pfn : `str`
        多站数据列表，单行是单站数据（字典格式）
    varkeys : `list`
        变量名列表
    method : `str`
        插值方法选择，可供选择的选项有'linear','nearest','cubic'，默认为'cubic'
    attr : `bool`
        在保存文件为json格式时生效的判断参数，该参数指示是否保存变量属性，若了False则输出文件
        只保存数据而不保存属性，若为True则也保存属性
    savepath : `str`
        保存路径，默认为None，若为None则返回数据字典和属性字典，若不为None则保存文件且函数
        无返回值。

    返回值
    -----
    `None` | 'tuple' : 如果设置了savepath参数，则函数根据savepath保存文件并返回None，
                       如果savepath参数为None，则函数返回一个由两个字典组成的元组，其结构
                       为(data_dict,attr_dict)，其中data_dict是数据字典，attr_dict是
                       属性字典

    错误
    ---
    OutputError : 当参数savepath不以'.json'或'.nc'结尾时抛出
    '''
    def get_datetime(pfn):
        timestr = pfn.split('/')[-1].split('.')[0]
        yyyy = int(timestr[:4])
        mm = int(timestr[4:6])
        dd = int(timestr[6:8])
        HH = int(timestr[8:10])
        MM = int(timestr[10:])
        time_obj = datetime.datetime(yyyy,mm,dd,HH,MM)
        time_units = 'minutes since 2018-01-01 00:00:00'
        return nc.date2num(time_obj,time_units)

    def refill_negative(data_array):
        '''剔除负值'''
        for ny, r in enumerate(data_array):
            for nx, c in enumerate(r):
                if c < 0:
                    data_array[ny][nx] = 0
        return data_array

    def nan_convert(array,to=None):
        '''将字典数据中的nan替换成None'''
        # for key in data_dict:
        if type(array) == float:
            return array
        elif len(np.array(array).shape) == 3:
            for nl, l in enumerate(array):
                for ny, r in enumerate(l):
                    for nx, c in enumerate(r):
                        try:
                            int(c)
                        except:
                            array[nl][ny][nx] = to
        elif len(np.array(array).shape) == 1:
            return array

        return array

    def save2json(data_dict,attr_dict,attr,savepath):
        '''保存为json文件'''
        from json import dumps

        dataset = {}
        for key in data_dict:
            # data_array = nan_convert(data_dict[key])
            try:
                data_list = nan_convert(data_dict[key].tolist())
            except AttributeError:
                pass
            if attr == True:
                dataset[key] = {'data':data_list,'attribute':attr_dict[key]}
            else:
                dataset[key] = data_list
        js_str = js.dumps(dataset)
        with open(savepath,'w') as f:
            f.write(js_str)

    dataset = multi_v_interp(load_js(pfn))
    sh = std_sh()

    min_lon = 85
    max_lon = 125
    min_lat = 14
    max_lat = 45
    grd_lon = np.arange(min_lon,max_lon,0.5)
    grd_lat = np.arange(min_lat,max_lat,0.5)
    grd_lons, grd_lats = np.meshgrid(grd_lon,grd_lat)

    data_dict = {}
    for varkey in varkeys:
        multigrds = []
        for height in sh:
            sh_index = sh.index(height)
            lon = []
            lat = []
            values = []

            for line in dataset:
                try:
                    int(line[varkey][sh_index])
                except ValueError:
                    continue
                else:
                    lon.append(line['lon'])
                    lat.append(line['lat'])
                    values.append(line[varkey][sh_index])

            lon = np.array(lon)
            lat = np.array(lat)

            values = np.array(values)
            try:
                grds = griddata((lon,lat),values,(grd_lons,grd_lats),
                                method=method)
            except:
                grds = np.full(grd_lons.shape,np.nan)

            # issue1 here
            grds = nan_convert(grds,to=-9999)

            # 在cubic算法下，水平风速会被插值出负值
            # 这时候需要对负值进行剔除，按0值处理
            if varkey == 'HWS':
                grds = refill_negative(grds)

            multigrds.append(grds)
            data_dict[varkey] = np.array(multigrds)

    data_dict['lon'] = grd_lon
    data_dict['lat'] = grd_lat
    data_dict['level'] = np.array(sh)
    data_dict['time'] = get_datetime(pfn)

    attr_dict = get_attr_dict()

    if savepath:
        if savepath.endswith('.nc'):
            save_as_nc(data_dict,attr_dict,savepath)
            return None
        elif savepath.endswith('.json'):
            save2json(data_dict,attr_dict,attr,savepath)
            return None
        else:
            raise OutputError('Saving file type Error. Only support file types'\
                              ' of .nc and .json.')
    else:
        return data_dict,attr_dict


def main():
    pass


if __name__ == '__main__':
    main()
