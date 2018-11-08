# coding : utf-8
'''
--------------------------------------------------------------------
项目名：rwp
模块名：algom.io
该模块包含了本项目与输入输出相关的所有函数
--------------------------------------------------------------------
python = 3.6
依赖库：
    pandas        $ conda install pandas
    netCDF4       $ conda install netCDF4
--------------------------------------------------------------------
'''
import json as js
import pandas as pd
import netCDF4 as nc
import datetime
import pdb


def load_js(filepath):
    '''加载json数据'''
    with open(filepath) as file_obj:
        raw_content = file_obj.readlines()

    data = [js.loads(line) for line in raw_content]

    return data


def parse(pfn):
    '''产品文件解析
    该函数可以自动识别产品类型（WNDROBS，WNDHOBS，WNDOOBS）

    输入参数
    -------
    pfn : `string`
        路径文件名

    返回值
    -----
    dataset : `dictionary`
        数据集字典，既包含数据也包含属性信息
    '''
    # 融合数据和属性信息
    try:
        data = parse_data(pfn)
        info = parse_info(pfn)
    except:
        return None

    dataset = data
    dataset.update(info)

    return dataset


def parse_data(pfn):
    '''
    该函数用于读取风廓线雷达产品数据（OBS）

    实时采样高度（ROBS）
    风廓线雷达实时的采样高度上的产品数据文件包括两部分内容，一部分是参考信息即测站基本参
    数；另一部分是产品数据实体部分，包括每个采样高度上的所获得的数据，包括采样高度、水平
    风向、水平风速、垂直风速、水平方向可信度、垂直方向可信度、Cn2。

    风廓线雷达通用数据格式说明：
    http://2010973.itpcas.ac.cn/UploadFiles/201307/files/Up2013717153635.pdf

    输入参数
    -------
    pfn : `string`
        路径文件名

    返回值
    -----
    data_dict : `dictionary`
        读取的数据字典
        变量名的解释

        SH : Sampling Height                    采样高度
        HWD : Horizontal Wind Direction         水平风向（°）
        HWS : Horizontal Wind Speed             水平风速（m/s）
        VWS : Vertical Wind Speed               垂直风速（m/s）
        HDR : Horizontal Direction Reliability  水平风向可信度（%）
        VDR : Vertical Direction Reliability    垂直风向可信度（%）
        CN2 :                                   折射率结构常数


    示例
    ----
    In [1]: from wprio import parse_data

    In [2]: robspath = './data/Z_RADA_I_G7190_20180809234508_P_WPRD_LC_ROBS.TXT'

    In [3]: data = parse_data(robspath)

    In [4]: list(data.keys())
    Out[4]: ['SH', 'HWD', 'HWS', 'VWS', 'HDR', 'VDR', 'CN2']
    '''
    data_df = pd.read_csv(pfn, sep=' ', skiprows=3, skipfooter=1,
                      names=['SH', 'HWD', 'HWS', 'VWS', 'HDR', 'VDR',
                             'CN2'], engine='python')

    # 由于当文件中含有/////时会以字符串格式导入，因此把字符串格式转化为数字类型
    # /////会被转换为 NaN
    data_df = data_df.apply(pd.to_numeric, errors='coerce')

    # 把 NaN 类型转化为内建 None 类型，以便后期以null输出到json文件
    data_df = data_df.where(pd.notnull(data_df), None)

    data_dict = data_df.to_dict('list')

    return data_dict


def parse_info(pfn):
    '''
    该函数用于读取雷达数据文件的头部信息，包括站号、经纬度、海拔高度、波段、时间。

    输入参数
    -------
    pfn : `string`
        路径文件名

    返回值
    -----
    result : `dictionary`
        包含该数据文件的站号（station）、经度（lon）、纬度（lat）、海拔高度（altitude）、
        波段（wave）、时间（time）的字典。
    '''
    # 判断产品种类
    with open(pfn,'r') as fileobj:
        first_line = fileobj.readline()
    kind = first_line.strip().split(' ')[0]

    def item_num(item):
        '''用于识别项索引位置'''
        if item:
            if len(item) == 5:
                result = 0
            elif len(item) == 9:
                result = 1
            elif len(item) == 8:
                result = 2
            elif len(item) == 7:
                result = 3
            elif len(item) == 2:
                result = 4
            elif len(item) == 14:
                result = 5
            else:
                result = None
        else:
            result = None

        return result

    def fill_miss(items):
        '''用递归算法在缺失项插入None'''
        if len(items) < 6:

            expect_items_num = set([0,1,2,3,4,5])
            exist_items_num = set([])
            for item in items:
                num = item_num(item)
                if num != None:
                    exist_items_num.add(num)
                else:
                    pass
            miss_items = list(expect_items_num - exist_items_num)
            miss_items.sort()
            miss_item = miss_items[0]
            items.insert(miss_item,None)

            return fill_miss(items)
        else:
            return items

    with open(pfn, encoding='utf-8', errors='ignore') as fileobj:
        content = fileobj.readlines()

    items = content[1].strip().split(' ')
    exist_items = [item for item in items if item]

    # 用递归算法在缺失项插入None
    expect_items = fill_miss(exist_items)

    stn, lon, lat, hgt, wav, time = expect_items

    result = {'station': stn, 'lon': float(lon), 'lat': float(lat),
              'altitude': float(hgt), 'wave': wav, 'time': time,
              'type':kind}

    return result


def save_as_json(data, path_fn, mod='single'):
    '''
    该函数用于将数据保存为json格式

    输入参数
    -------
    data : `dictionary | list`
        输入数据
        在mod参数为'multi'的情况下，输入数据结构需满足如下要求：
        `dictionary`:
            {'line1':
                {'label1':[label1's data],
                 'label2':[label2's data],
                 ...

                 'labeln':[labeln's data]
                }
             'line2':
                {'label1':[label1's data],
                 'label2':[label2's data],
                 ...

                 'labeln':[labeln's data]
                }
             ...

             'linen':
                {'label1':[label1's data],
                 'label2':[label2's data],
                 ...

                 'labeln':[labeln's data]
                }
            }

        `list`:
            [
             {'label1':[label1's data],
              'label2':[label2's data],
              ...

              'labeln':[lablen's data]
             },

             ...

             {'label1':[label1's data],
              'label2':[label2's data],
              ...

              'labeln':[lablen's data]
             }
            ]
        注：
        1.'line1', 'line2'...为自定义行识别键，仅用于行识别，在最终的json文件中不会显示该
            键。
        2.'label1','label2'...为数据要素名称键，该键须在每个记录行都保持一致。

    path_fn : `string`
        文件保存路径

    mod : `string`
        模式选择，可设为'multi'或'single'，若输入数据为单个字典，则取single模式，若为多字典
        嵌套，则选multi模式。
    '''
    if mod == 'multi':
        multi_data = data

        result_list = []

        if type(multi_data) == dict:
            for key in multi_data:
                result_list.append(js.dumps(multi_data[key]))
        elif type(multi_data) == list:
            for line in multi_data:
                result_list.append(js.dumps(line))

        result_js = '\n'.join(result_list)
    elif mod == 'single':
        result_js = js.dumps(data)

    with open(path_fn, 'w') as fileobj:
        fileobj.write(result_js)


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
    `bool` : 是否处理成功的标识，若顺利完成，返回True
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
            opt_data[key][:] = data_dict[key]
            try:
                opt_data[key].setncatts(attr_dict[key])
            except KeyError:
                pass
            opt_data[key].copyright = copyright

    return True


if __name__ == '__main__':
    pass
