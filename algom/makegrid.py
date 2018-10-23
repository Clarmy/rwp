# coding:utf-8
import json as js
import numpy as np
import netCDF4 as nc
from scipy.interpolate import griddata
from scipy.interpolate import interp1d
import ipdb


def load_data(filepath):
    '''加载json数据'''
    with open(filepath) as file_obj:
        raw_content = file_obj.readlines()

    data = [js.loads(line) for line in raw_content]

    return data


def veticl_interpolate(single_ds):
    '''垂直插值单站数据集

    输入参数
    -------
    single_ds : `dict`
        单站数据字典，包含有高度、站点信息及需插值变量等变量。

    返回值
    -----
    new_single_ds : `dict`
        经过垂直插值处理后的单站数据集，经处理后其高度层为统一结构（100-10000,42层），
        缺省值为np.nan。
    '''
    # 制作标准高度层
    sh1 = range(100,2000,100)
    sh2 = range(2000,5000,250)
    sh3 = range(5000,10000,500)
    shtop = 10000
    sh = list(sh1)+list(sh2)+list(sh3)
    sh.append(shtop)

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


def multi_station_vetcl_intp(dataset):
    for line in dataset:
        


def horiz_interpolate():
    pass


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
    if dim_num == 3:
        src_keys.remove('level')

    src_lon = data_dict['lon']
    src_lat = data_dict['lat']
    if dim_num == 3:
        src_level = data_dict['level']

    with nc.Dataset(savepath, 'w') as file_obj:

        file_obj.createDimension('lon', len(src_lon))
        file_obj.createDimension('lat', len(scr_lon))
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
            opt_data[key][:] = data_dict[key]
            opt_data[key].setncattrs(attr_dict[key])
            opt_data[key].copyright = copyright

    return True


def main():
    pass


if __name__ == '__main__':
    main()
