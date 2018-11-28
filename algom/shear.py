# coding:utf-8
'''
--------------------------------------------------------------------
项目名：rwp
模块名：algom.shear
本模块用于进行风切变分析
--------------------------------------------------------------------
python = 3.6
依赖库：
    numpy       $ conda install numpy
    netCDF4     $ conda install netCDF4
    scipy       $ conda install scipy
--------------------------------------------------------------------
'''
import sys
sys.path.append('..')

import numpy as np
import netCDF4 as nc

from scipy.interpolate import interp1d
from algom.io import save_as_nc
from algom.errors import InputError


def get_attr_dict():
    attr_dict = {'SHR_U':{'long_name':'Shear of U along vertical direction.',
                        'units':'(m/s)/(100*m)',
                        'fill_value':-9999.,
                        'note':'U and V\'s direction is that wind blows to,'\
                        ' rather than wind comes from.' },
                 'SHR_V':{'long_name':'Shear of V along vertical direction.',
                        'units':'(m/s)/(100*m)',
                        'fill_value':-9999.,
                        'note':'U and V\'s direction is that wind blows to,'\
                        ' rather than wind comes from.'},
                 'SHR_VWS':{'long_name':'Shear of Vertical Wind Speed '\
                            'along vertical direction',
                        'units':'(m/s)/(100*m)',
                        'fill_value':-9999,
                        'note':'Positive is downward, negative is upward.'},
                 'level':{'long_name':'Sampling height level',
                        'units':'m'},
                 'SHR_HWS':{'long_name':'Shear of Horizontal Wind Speed '\
                            'along vertical direction',
                        'units':'(m/s)/(100*m)',
                        'fill_value':-9999},
                 'SHR_HWD':{'long_name':'Shear of Horizontal Wind Direction '\
                            'along vertical direction',
                        'units':'degree/(100*m)',
                        'fill_value':-9999,
                        'note':'Values increase clockwise from north. '\
                        'The value denotes the direction that wind comes from.'},
                 'lon':{'long_name':'longitudes','units':'degree_east'},
                 'lat':{'long_name':'latitudes','units':'degree_north'},
                 'time':{'long_name':'datetime'}}
    return attr_dict


def single_shear(index,array,delt=100,mod='normal'):
    '''计算变量的切变

    输入参数
    -------
    index : `list` | `ndarray`
        变量索引值
    array : `list` | `ndarray`
        变量数据值，其长度应与index对应相同

    返回值
    -----
    `ndarray` : 垂直切变值
    '''

    new_index = []
    new_array = []
    for i in range(len(index)):
        if array[i] and not np.isnan(array[i]):
            new_index.append(index[i])
            new_array.append(array[i])

    try:
        model = interp1d(new_index,new_array,kind='quadratic',
                        fill_value=np.nan,bounds_error=False)
    except ValueError:
        nan_array = np.full(np.array(index).shape,np.nan)
        return nan_array

    shr = []
    # 下边界处理
    shr.append(model(index[0]+delt) - model(index[0]))
    # 主体数据处理
    for i in index[1:-1]:
        shr.append(model(i+delt*0.5) - model(i-delt*0.5))

    # 上边界处理
    shr.append(model(index[-1]) - model(index[-1]-delt))
    if mod == 'direction':
        for n,dirc in enumerate(shr):
            if abs(dirc) > 180:
                shr[n] = dirc - 360
    result = np.array(shr)

    return result


def multi_shear(index,array,axis='height',mod='normal'):
    '''计算3维切变

    输入参数
    -------
    index : `ndarray` | `list`
        切变轴上的相应变量值，例如高度轴上的高度值。该变量为1维数组。
    array : `ndarray`
        待计算的三维数组，该数组第1维(最左端)必须为高度
    axis : `str`
        切变轴选择，可以选择沿高度:'height'，沿经向（纬圈）:'lon'，
        沿纬向（经圈）:'lat'
    '''
    if type(array) != np.ndarray:
        array = np.array(array)
    if type(index) != np.ndarray:
        index = np.array(index)
    if len(array.shape) != 3:
        raise ValueError('array is not 3-Dimensions')
    if len(index.shape) != 1:
        raise ValueError('index is not 1-Dimension')

    shape = array.shape
    np.place(array,array==-9999,np.nan)
    shear_array = np.full(shape,np.nan,dtype=np.float64)
    for r in range(shape[1]):
        for c in range(shape[2]):
            shear_array[:,r,c] = single_shear(index,array[:,r,c],
                                              mod=mod)

    np.place(shear_array,np.isnan(shear_array),-9999)
    return shear_array


def full_wind_shear(readpfn,savepfn):
    '''处理整个时次的风切变

    输入参数
    -------
    readpfn : `str`
        输入文件路径，文件须为.nc文件
    savepfn : `str`
        输出文件路径，文件须为.nc文件

    '''
    if not readpfn.endswith('.nc'):
        raise InputError('Input file is not the type of netCDF.')

    file_obj = nc.Dataset(readpfn)
    lat = file_obj.variables['lat'][:]
    lon = file_obj.variables['lon'][:]
    time = file_obj.variables['time'][:]
    height = file_obj.variables['level'][:]

    u = file_obj.variables['U'][:]
    v = file_obj.variables['V'][:]
    hws = file_obj.variables['HWS'][:]
    hwd = file_obj.variables['HWD'][:]
    vws = file_obj.variables['VWS'][:]

    sh_u = multi_shear(height,u)
    sh_v = multi_shear(height,v)
    sh_hws = multi_shear(height,hws)
    sh_hwd = multi_shear(height,hwd, mod='direction')
    sh_vws = multi_shear(height,vws,)

    data_dict = {'lon':lon, 'lat':lat, 'level':height, 'time':time,
                 'SHR_U':sh_u, 'SHR_V':sh_v, 'SHR_HWS':sh_hws,
                 'SHR_HWD':sh_hwd,'SHR_VWS':sh_vws}

    attr_dict = get_attr_dict()

    save_as_nc(data_dict,attr_dict,savepfn)


def main():
    pass

if __name__ == '__main__':
    main()
