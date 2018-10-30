# coding : utf-8
'''
--------------------------------------------------------------------
项目名：WDPF ( WinD ProFile )
模块名：wprio (Input & Output)
该模块包含了本项目与输入输出相关的所有函数
--------------------------------------------------------------------
python = 3.6
依赖库：
    pandas        $ conda install pandas
--------------------------------------------------------------------
 李文韬   |   liwentao@mail.iap.ac.cn   |   https://github.com/Clarmy
--------------------------------------------------------------------
'''
import json as js
import pandas as pd
import pdb


def read(pfn, mod='ROBS'):
    '''
    该函数用于读取风廓线雷达数据

    风廓线雷达数据分为经向数据文件（RAD)和实时采样高度数据文件（ROBS）

    经向数据（RAD）
    风廓线雷达径向数据文件包括两部分内容，一部分是参考信息即测站基本参数、雷达性能参数、
    观测参数；另一部分是观测数据实体部分，包括每个波束在每个采样高度上的观测数据，包括采
    样高度、速度谱宽、信噪比、径向速度。

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
    mod : `string`
        读取模式，可选参数为'ROBS'、'RAD'，默认值为'ROBS'

    返回值
    -----
    data_dict : `dictionary`
        读取的数据字典

            若 mod = 'ROBS'，则返回值为1个字典
                其键值对结构为        `string`:`list`

            若 mod = 'RAD'， 则返回值最多为3个字典，字典内嵌3层字典，
                最底层键值对结构为    `string`:`list`
                其余层键值对结构为    `string`:`dictionary`

                层级结构如下所示

                m1
                |-- m1B1
                |    |-- SH
                |    |-- VSW
                |    |-- SNR
                |    |-- RV
                ...
                |-- m1B5
                |    |-- SH
                |    |-- VSW
                |    |-- SNR
                |    |-- RV

                m2
                |-- m2B1
                |    |-- SH
                |    |-- VSW
                |    |-- SNR
                |    |-- RV
                ...
                |-- m2B5
                |    |-- SH
                |    |-- VSW
                |    |-- SNR
                |    |-- RV

                m3
                |-- m3B1
                |    |-- SH
                |    |-- VSW
                |    |-- SNR
                |    |-- RV
                ...
                |-- m3B5
                |    |-- SH
                |    |-- VSW
                |    |-- SNR
                |    |-- RV

            M : mod 雷达扫描模式，主要为高低模式
            B : beam 波束序号

        变量名的解释

        SH : Sampling Height                    采样高度
        VSW : Velocity Spectrum Width           速度谱宽
        SNR : Signal to Noise Ratio             信噪比
        RV : radial velocity                    径向速度
        HWD : Horizontal Wind Direction         水平风向（°）
        HWS : Horizontal Wind Speed             水平风速（m/s）
        VWS : Vertical Wind Speed               垂直风速（m/s）
        HDR : Horizontal Direction Reliability  水平风向可信度（%）
        VDR : Vertical Direction Reliability    垂直风向可信度（%）
        CN2 :                                   折射率结构常数


    示例
    ----
    In [1]: from wprio import read

    # RAD模式
    In [2]: radpath = './data/Z_RADA_I_51463_20180809000419_O_WPRD_LC_RAD.TXT'

    In [3]: m1, m2, m3 = read(radpath,mod='RAD')

    In [4]: list(m1.keys())
    Out[4]: ['m1B1', 'm1B2', 'm1B3', 'm1B4', 'm1B5']

    In [5]: list(m2.keys())
    Out[5]: ['m2B1', 'm2B2', 'm2B3', 'm2B4', 'm2B5']

    # ROBS模式
    In [6]: robspath = './data/Z_RADA_I_G7190_20180809234508_P_WPRD_LC_ROBS.TXT'

    In [7]: data = read(robspath,mod='ROBS')

    In [8]: list(data.keys())
    Out[8]: ['SH', 'HWD', 'HWS', 'VWS', 'HDR', 'VDR', 'CN2']
    '''
    def unformat(frag):
        '''
        片段内部数据反格式化解码
        '''
        result = {'SH': [], 'VSW': [], 'SNR': [], 'RV': []}
        keys = ['SH', 'VSW', 'SNR', 'RV']

        for line in frag:
            for i, k in enumerate(keys):
                item = line.strip().split(' ')[i]
                try:
                    result[k].append(float(item))
                except ValueError:
                    result[k].append(None)

        return result

    if mod == 'ROBS':
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

    if mod == 'RAD':
        with open(pfn, encoding='utf-8', errors='ignore') as fileobj:
            content = fileobj.readlines()

        # 提取每个片段的首尾索引值，并zip
        frag_head = []
        frag_tail = []
        for index, line in enumerate(content):
            if line.startswith('RAD'):
                frag_head.append(index)
            elif line.startswith('NNNN'):
                frag_tail.append(index)
        frag_index = zip(frag_head, frag_tail)

        # 根据每个片段的首尾索引值提取主体数据
        bodys = []
        for frag in frag_index:
            headn = frag[0]+1
            tailn = frag[1]
            bodys.append(unformat(content[headn:tailn]))

        # 检查数据片段数是否为5、10或15，若不是则直接结束该次调用
        try:
            assert len(bodys) in [5, 10, 15]
        except AssertionError:
            return None, None, None

        # 制作每个片段的键名
        keys1 = ['m1B{0}'.format(b+1) for b in range(5)]
        if len(bodys) == 10:
            keys2 = ['m2B{0}'.format(b+1) for b in range(5)]
        if len(bodys) == 15:
            keys3 = ['m3B{0}'.format(b+1) for b in range(5)]

        # 构建3种模式字典
        m1_dict = {}
        for index, key in enumerate(keys1):
            m1_dict[key] = bodys[index]

        m2_dict = None
        m3_dict = None
        if len(bodys) == 10:
            m2_dict = {}
            for index, key in enumerate(keys2):
                m2_dict[key] = bodys[index+5]
        elif len(bodys) == 15:
            m3_dict = {}
            for index, key in enumerate(keys3):
                m3_dict[key] = bodys[index+10]

        data_dict = m1_dict, m2_dict, m3_dict

    return data_dict


def head_info(pfn, mod='ROBS'):
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

    if mod == 'ROBS':
        items = content[1].strip().split(' ')
        exist_items = [item for item in items if item]

        # 用递归算法在缺失项插入None
        expect_items = fill_miss(exist_items)

        stn, lon, lat, hgt, wav, time = expect_items

    elif mod == 'RAD':
        stn, lon, lat, hgt, wav = content[1].strip().split(' ')
        time = content[3].split(' ')[1]

    result = {'station': stn, 'lon': float(lon), 'lat': float(lat),
              'altitude': float(hgt), 'wave': wav, 'time': time}

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


if __name__ == '__main__':
    pass
