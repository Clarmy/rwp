# coding:utf-8
import json as js
import numpy as np
import netCDF4 as nc
from scipy.interpolate import griddata
from scipy.interpolate import interp1d
import datetime
import ipdb


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
                 'level':{'long_name':'Sampling Height level',
                        'units':'m'},
                 'lon':{'long_name':'longitudes','units':'degree_east'},
                 'lat':{'long_name':'latitudes','units':'degree_north'},
                 'time':{'long_name':'datetime',
                         'units':'minutes since 2018-01-01 00:00:00'}}
    return attr_dict


def load_data(filepath):
    '''加载json数据'''
    with open(filepath) as file_obj:
        raw_content = file_obj.readlines()

    data = [js.loads(line) for line in raw_content]

    return data


def std_sh():
    '''获取标准采样高度层'''
    sh1 = range(100,2000,100)
    sh2 = range(2000,5000,250)
    sh3 = range(5000,9500,500)
    sh = list(sh1)+list(sh2)+list(sh3)

    return sh


def veticl_interpolate(single_ds):
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


def multi_station_vetcl_intp(raw_dataset):
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
    return [veticl_interpolate(line) for line in raw_dataset]


def complete_interpolate(pfn,varkeys=['HWD', 'HWS', 'VWS', 'HDR',
    'VDR', 'CN2'],savepath=None):
    '''多层水平插值

    输入参数
    -------
    pfn : `str`
        多站数据列表，单行是单站数据（字典格式）
    varkeys : `list`
        变量名列表
    savepath : `str`
        保存路径，默认为None，若为None则返回数据字典和属性字典，若不为None则保存文件且函数
        无返回值。

    返回值
    -----
    `ndarray`

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

    dataset = multi_station_vetcl_intp(load_data(pfn))
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

            grds = griddata((lon,lat),values,(grd_lons,grd_lats),method='linear')
            multigrds.append(grds)
            data_dict[varkey] = np.array(multigrds)

    data_dict['lon'] = grd_lon
    data_dict['lat'] = grd_lat
    data_dict['level'] = np.array(sh)
    data_dict['time'] = get_datetime(pfn)

    attr_dict = get_attr_dict()

    if savepath:
        save_as_nc(data_dict,attr_dict,savepath)
        return None
    else:
        return data_dict,attr_dict


def save_as_nc(data_dict, attr_dict, savepath):
    '''将数据字典和属性字典融合保存为netCDF4文件

    输入参数
    -------
    data_dict : `dict`
        数据字典，其中必须包括('lon','lat','time')三个辅助变量和至少一个数据变量，如果数据是
          三维数组，则辅助变量还需要包含('level'), 数据变量内须为('time','lat','lon')或
          ('time','level','lat','lon')格式数组，类型须为numpy.ndarray。

    attr_dict : `dict`
        属性字典，双层嵌套型字典，顶层键为'lon','lat','time','level'等变量名，其对应值为该
          变量的属性字典，为保险起见，避免不必要的异常，最好属性字典的键和值都以str的格式存储。
          注意：attr_dict的顶层键须与data_dict的键完全一致。

    savepath : `str`
        输出nc文件保存的完整路径, 须包含文件名及后缀。例如'./output/data.nc'

    返回值
    -----
    `bool` : 是否处理成功的标识，若顺利完成，返回True，否则返回False

    示例
    ----
    暂无

    '''
    # 判断数据是三维还是二维
    dim_num = 3
    try:
        data_dict['level']
    except KeyError:
        dim_num = 2

    copyright = 'This netCDF4 dataset is parsed, processed and packaged by '\
        'Beijing Presky Inc., contact us please visit : http://www.cnpresky.com'

    src_keys = set(data_dict.keys())
    src_keys.remove('lon')
    src_keys.remove('lat')
    src_keys.remove('time')
    if dim_num == 3:
        src_keys.remove('level')

    src_lon = data_dict['lon']
    src_lat = data_dict['lat']
    if dim_num == 3:
        src_level = data_dict['level']

    with nc.Dataset(savepath, 'w') as file_obj:

        file_obj.createDimension('lon', len(src_lon))
        file_obj.createDimension('lat', len(src_lat))
        file_obj.createDimension('time', None)
        if dim_num == 3:
            file_obj.createDimension('level', len(src_level))

        opt_data = {}
        opt_data['lat'] = file_obj.createVariable('lat', float, ('lat',))
        opt_data['lon'] = file_obj.createVariable('lon', float, ('lon',))
        opt_data['level'] = file_obj.createVariable('level', float, ('level',))
        opt_data['time'] = file_obj.createVariable('time', float, ('time',))

        for key in src_keys:
            opt_data[key] = file_obj.createVariable(key, float,
                                                    ('level', 'lat', 'lon'))

        for key in data_dict:
            # try:
            opt_data[key][:] = data_dict[key]
            try:
                opt_data[key].setncatts(attr_dict[key])
            except KeyError:
                pass
            opt_data[key].copyright = copyright
            # except:
            #     ipdb.set_trace()

    return True


def main():
    pass


if __name__ == '__main__':
    main()