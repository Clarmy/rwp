# coding:utf-8
'''
--------------------------------------------------------------------
项目名：rwp
模块名：makegrid
本模块用于对不规则站点数据进行格点化插值处理
--------------------------------------------------------------------
python = 3.6
依赖库：
    numpy         $ conda install numpy
    netCDF4       $ conda install netCDF4
    scipy         $ conda install scipy
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


class OutputError(Exception):
    '''输出错误'''
    def __init__(self, message):
        self.message = message


def get_attr_dict():
    attr_dict = {'U':{'long_name':'U component of wind.',
                        'units':'m/s',
                        'note':'U and V\'s direction is that wind blows to,'\
                        ' rather than wind comes from.' },
                 'V':{'long_name':'V component of wind.',
                        'units':'m/s',
                        'note':'U and V\'s direction is that wind blows to,'\
                        ' rather than wind comes from.'},
                 'VWS':{'long_name':'Vertical Wind Speed',
                        'units':'m/s',
                        'note':'Positive is downward, negative is upward.'},
                 'level':{'long_name':'Sampling height level',
                        'units':'m'},
                 'HWS':{'long_name':'Horizontal Wind Speed',
                        'units':'m/s'},
                 'HWD':{'long_name':'Horizontal Wind Direction',
                        'units':'degree',
                        'note':'Values increase clockwise from north. '\
                        'The value denotes the direction that wind comes from.'},
                 'lon':{'long_name':'longitudes','units':'degree_east'},
                 'lat':{'long_name':'latitudes','units':'degree_north'},
                 'time':{'long_name':'datetime',
                         'units':'minutes since 2018-01-01 00:00:00'}}
    return attr_dict


def sd2uv(ws,wd):
    '''风速风向转化为uv场'''
    u = ws * np.sin(np.deg2rad(wd))
    v = ws * np.cos(np.deg2rad(wd))

    return u,v


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


def full_interp(pfn, method='linear', attr=False, savepath=None):
    '''在单个站点垂直插值的基础上对所有站点所有层次进行插值处理

    输入参数
    -------
    pfn : `str`
        多站数据列表，单行是单站数据（字典格式）
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
    # for varkey in varkdeys:
    multi_u_grds = []
    multi_v_grds = []
    multi_hws_grds = []
    multi_hwd_grds = []
    multi_vws_grds = []
    for height in sh:
        sh_index = sh.index(height)

        hwd = []
        hws = []
        hz_lon = []
        hz_lat = []

        vws = []
        vt_lon = []
        vt_lat = []

        for line in dataset:
            try:
                int(line['HWD'][sh_index])
                int(line['HWS'][sh_index])
            except ValueError:
                continue
            else:
                hws.append(line['HWS'][sh_index])
                hwd.append(line['HWD'][sh_index])
                hz_lon.append(line['lon'])
                hz_lat.append(line['lat'])

            try:
                int(line['VWS'][sh_index])
            except ValueError:
                continue
            else:
                vws.append(line['VWS'][sh_index])
                vt_lon.append(line['lon'])
                vt_lat.append(line['lat'])

        hz_lon = np.array(hz_lon)
        hz_lat = np.array(hz_lat)

        vt_lon = np.array(vt_lon)
        vt_lat = np.array(vt_lat)

        hwd = np.array(hwd,dtype=np.float64)
        hws = np.array(hws,dtype=np.float64)
        vws = np.array(vws,dtype=np.float64)

        u,v = sd2uv(hws,hwd)

        try:
            u_grds = griddata((hz_lon,hz_lat),u,(grd_lons,grd_lats),
                            method=method)
        except:
            u_grds = np.full(grd_lons.shape,np.nan)
        try:
            v_grds = griddata((hz_lon,hz_lat),v,(grd_lons,grd_lats),
                            method=method)
        except:
            v_grds = np.full(grd_lons.shape,np.nan)
        try:
            vws_grds = griddata((vt_lon,vt_lat),vws,(grd_lons,grd_lats),
                            method=method)
        except:
            vws_grds = np.full(grd_lons.shape,np.nan)

        hws_grds = np.sqrt(u_grds**2 + v_grds**2)
        hwd_grds = np.rad2deg(np.arcsin(u_grds/hws_grds))

        # 风的来向与去向转换
        u_grds = -u_grds
        v_grds = -v_grds

        # issue1 here
        u_grds = np.ma.masked_invalid(u_grds)
        v_grds = np.ma.masked_invalid(v_grds)
        hws_grds = np.ma.masked_invalid(hws_grds)
        hwd_grds = np.ma.masked_invalid(hwd_grds)
        vws_grds = np.ma.masked_invalid(vws_grds)

        multi_u_grds.append(u_grds)
        multi_v_grds.append(v_grds)
        multi_vws_grds.append(vws_grds)
        multi_hws_grds.append(hws_grds)
        multi_hwd_grds.append(hwd_grds)

    data_dict['U'] = np.ma.array(multi_u_grds,dtype=np.float64)
    data_dict['V'] = np.ma.array(multi_v_grds,dtype=np.float64)
    data_dict['VWS'] = np.ma.array(multi_vws_grds,dtype=np.float64)
    data_dict['HWS'] = np.ma.array(multi_hws_grds,dtype=np.float64)
    data_dict['HWD'] = np.ma.array(multi_hwd_grds,dtype=np.float64)
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
