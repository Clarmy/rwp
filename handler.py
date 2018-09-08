# coding : utf-8
'''
--------------------------------------------------------------------
项目名：WDPF ( WinD ProFile )
模块名：handler
该模块包含了本项目与数据预处理有关的函数
--------------------------------------------------------------------
python = 3.6
version = 0.0.1
依赖库：
    numpy        $ conda install numpy
--------------------------------------------------------------------
 李文韬   |   liwentao@mail.iap.ac.cn   |   https://github.com/Clarmy
--------------------------------------------------------------------
'''
from sys import argv
from io import read
from io import head_info
from io import save_as_json
import numpy as np
import pandas as pd


def nan_2_none(func):
    '''
    用于将np.nan空值类型转化为内建None类型的装饰器，目前只能用于字典类型，字典格式为一层嵌套，底层格式为列表。
    '''
    def wrapper(*args, **kwargs):
        result_dict = func(*args, **kwargs)
        for k in result_dict:
            for i in range(len(result_dict[k])):
                if pd.isnull(result_dict[k][i]):
                    result_dict[k][i] = None
        return result_dict
    return wrapper


def multi_beam_mean(mod_data):
    '''
    计算单个模式中的多波束平均
    '''
    result_dict = {}
    keys = ['SH', 'VSW', 'SNR', 'RV']

    for index, mod in enumerate(mod_data):
        for key in keys:
            if index == 0:
                result_dict[key] = np.array(mod_data[mod][key]).astype(np.float64)
            else:
                result_dict[key] += np.array(mod_data[mod][key]).astype(np.float64)

    for key in keys:
        result_dict[key] /= 5

    return result_dict


@nan_2_none     # 把 NaN 类型转化为None以便输出 json 时转化为 null
def multi_mod_mean(data):
    '''
    计算多模式平均
    '''
    mod1, mod2, mod3 = data
    general = {}

    try:
        general['mod1'] = multi_beam_mean(mod1)
        general['mod2'] = multi_beam_mean(mod2)
        general['mod3'] = multi_beam_mean(mod3)
    except TypeError:
        pass

    dfs = {k: pd.DataFrame(general[k]) for k in general}

    for index, key in enumerate(dfs):
        if index == 0:
            ndf = dfs[key]
        else:
            ndf = pd.merge(ndf, dfs[key], how='outer')

    ndf = ndf.sort_values('SH', axis=0)
    result_dict = ndf.to_dict('list')

    return result_dict


def judge_mod(pfn):
    '''
    用于判断读取模式
    '''
    return pfn.split('.')[-2].split('_')[-1]


def proc_wrap(pfn, func, mod):
    '''
    process and wrap处理打包函数
    用于使用自定义函数处理数据并封装描述信息
    '''
    data = read(pfn, mod)
    info = head_info(pfn, mod)
    if mod == 'RAD':
        result = func(data)
    else:
        result = data
    result.update(info)

    return result


def main(inpath, outpath):
    '''
    主函数
    '''
    mod = judge_mod(inpath)
    result = proc_wrap(inpath, multi_mod_mean, mod)
    save_as_json(result, outpath, mod='single')


if __name__ == '__main__':
    INPATH, OUTPATH = argv[1], argv[2]
    # INPATH = './data/ROB/Z_RADA_I_G7190_20180809234508_P_WPRD_LC_ROBS.TXT'
    # OUTPATH = './test.json'
    main(INPATH, OUTPATH)
