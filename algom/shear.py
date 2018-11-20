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

def single_shear(index,array,delt=1):
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
    shr.append((model(index[0]+delt) - model(index[0]))/delt)
    # 主体数据处理
    for i in index[1:-1]:
        shr.append((model(i+delt) - model(i-delt)) / (2 * delt))

    # 上边界处理
    shr.append((model(index[-1]) - model(index[-1]-delt))/delt)

    return np.array(shr)


def multi_shear(index,array,axis='height'):
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
            shear_array[:,r,c] = single_shear(index,array[:,r,c])

    return shear_array


def main():
    pass

if __name__ == '__main__':
    main()
