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
--------------------------------------------------------------------
'''
import sys
sys.path.append('..')

import numpy as np
import netCDF4 as nc

from scipy.interpolate import interp1d
from algom.io import save_as_nc

def shear(index,array,delt=1):
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

    model = interp1d(new_index,new_array,kind='quadratic',
                     fill_value=np.nan,bounds_error=False)

    shr = []
    # 下边界处理
    shr.append((model(index[0]+delt) - model(index[0]))/delt)
    # 主体数据处理
    for i in index[1:-1]:
        try:
            shr.append((model(i+delt) - model(i-delt)) / (2 * delt))
        except ValueError:
            import ipdb
            ipdb.set_trace()
    # 上边界处理
    shr.append((model(index[-1]) - model(index[-1]-delt))/delt)

    return np.array(shr)

def main():
    pass

if __name__ == '__main__':
    main()
