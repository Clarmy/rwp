# coding:utf-8
'''
--------------------------------------------------------------------
项目名：rwp
模块名：algom.diverge
本模块用于进行矢量的散度计算
--------------------------------------------------------------------
python = 3.6
依赖库：
    numpy       $ conda install numpy
    netCDF4     $ conda install netCDF4
--------------------------------------------------------------------
'''

import sys
sys.path.append('..')

import numpy as np
import netCDF4 as nc

from algom.io import save_as_nc


def lon_distance(latdeg):
    '''计算纬圈上单位经度的距离'''
    rad = np.deg2rad(latdeg)
    return np.cos(rad) * 11132300


def point_divg(ny,nx,u,v,interval=0.5,fill_value=-9999.):
    '''计算单格点的散度值

    输入参数
    -------
    ny : `float`
        单点坐标在Y轴上的索引值
    nx : `float`
        单点坐标在X轴上的索引值
    u : `ndarray`
        矢量场在X轴上的分量场，须为二维数组
    v : `ndarray`
        矢量场在Y轴上的分量场，须为二维数组
    interval : `float`
        矢量场间隔距离，可以是距离或经纬度间隔
    fill_value : `float`
        缺省值

    返回值
    -----
    `float` : 单点散度量，其单位为
              [原始量纲]/[interval*invterval量纲]
              例如：[u] = m/s, [v] = m/s, interval = 0.5°
              则输出值量纲为：(m/s)/(0.5°)
    '''

    if u[ny,nx+1] != fill_value and \
       u[ny,nx-1] != fill_value and \
       v[ny+1,nx] != fill_value and \
       v[ny-1,nx] != fill_value:
        Ax = (u[ny,nx+1]-u[ny,nx-1])/(interval*2)
        Ay = (v[ny+1,nx]-v[ny-1,nx])/(interval*2)
        A = Ax + Ay
    else:
        A = fill_value

    return A


def grid_divgs(u,v):
    '''计算多格点散度值

    输入参数
    -------
    u : `numpy.ndarray`
        风场U分量，或其他矢量的X轴分量，须为二维数组
    v : `numpy.ndarray`
        风场V分量，或其他矢量的Y轴分量，须为二维数组

    返回值
    -----
    `numpy.ndarray` : 格点散度，单位为
                      [原始量纲]/[interval*invterval量纲]
                      例如：[u] = m/s, [v] = m/s, interval = 0.5°
                      则输出值量纲为：(m/s)/(0.5°)
    '''
    shape = u.shape
    divs = np.full(shape,-9999)
    for ny in range(shape[0]):
        for nx in range(shape[1]):
            try:
                divs[ny,nx] = point_divg(ny,nx,u,v)
            except IndexError:
                continue

    return divs


def full_uv_divgs(pfn,savepath=None):
    '''对一个时次的拼图产品做完整的散度处理

    输入参数
    -------
    pfn : `str`
        输入文件路径，须包含文件名，且文件格式只支持nc
    savepath : `str`
        文件保存路径，须包含文件名，文件格式只支持nc

    返回值
    -----
    `None` | `dict` : 若savepath不存在，则返回两个字典（数据字典和属性字典），
                      否则保存文件并返回None
    '''
    def get_attr_dict(var_obj):
        '''从源数据获取属性字典'''
        attr_keys = var_obj.ncattrs()
        attr_dict = {}
        for key in attr_keys:
            attr_dict[key] = var_obj.getncattr(key)
        return attr_dict

    file_obj = nc.Dataset(pfn)
    u = file_obj.variables['U'][:]
    v = file_obj.variables['V'][:]
    lon = file_obj.variables['lon']
    lat = file_obj.variables['lat']
    time = file_obj.variables['time']
    level = file_obj.variables['level']

    attr_dict = \
    {
    'lon':get_attr_dict(lon),
    'lat':get_attr_dict(lat),
    'time':get_attr_dict(time),
    'level':get_attr_dict(level),
    'divs':{
        'long_name':'wind divergence.',
        'units':'(m/s)/(0.5°)',
        'fill_value':-9999.,
        'note':'Negative means convergence, positive means divergence'
          }
    }

    lon = lon[:]
    lat = lat[:]
    time = time[:]
    level = level[:]

    divs = []
    for n in range(len(level)):
        divs.append(grid_divgs(u[n],v[n]))

    divs = np.array(divs,dtype=np.float64)

    data_dict = \
    {
    'divs':divs,
    'lat':lat,
    'lon':lon,
    'time':time,
    'level':level
    }

    if savepath:
        save_as_nc(data_dict,attr_dict,savepath)
    else:
        return data_dict, attr_dict


def main():
    pass


if __name__ == '__main__':
    main()
