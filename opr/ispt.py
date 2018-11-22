'''
--------------------------------------------------------------------
项目名：rwp
模块名：opr.inspect
本模块用于检查文件情况
--------------------------------------------------------------------
python = 3.6
--------------------------------------------------------------------
'''
import sys
sys.path.append('..')

import os
import time
import json as js
from datetime import datetime
from opr.optools import check_dir
from opr.optools import standard_time_index

with open('../config.json') as f:
    config = js.load(f)

ROOT_PATH = config['parse']['oper']['save_path']
REPORT_PATH = '/mnt/data14/liwt/opr/parse/inspect/'
check_dir(REPORT_PATH)

def report(missing_set,pfn):
    '''生成缺失文件报告'''
    now = datetime.now().strftime('%Y%m%d%H%M%S')
    content = [now+'\n','missing:\n']
    missing_list = list(missing_set)
    missing_list.sort()
    for m in missing_list:
        content.append(m+'\n')

    with open(pfn,'w') as f:
        f.writelines(content)

    return True


def main():
    missing_set = set([])
    pre_folds = os.listdir(ROOT_PATH)
    pre_folds.sort()
    last_pre_fold = pre_folds[-1]
    while True:
        std_index = standard_time_index()
        folds = os.listdir(ROOT_PATH)
        folds.sort()
        last_fold = folds[-1]
        if last_fold == last_pre_fold:
            path = ROOT_PATH + last_fold + '/'
            filenames = os.listdir(path)
            indexs = [fn.split('.')[0] for fn in filenames]
            indexs.sort()
            last_time = indexs[-1]
            dampoint = std_index.index(last_time)
            if dampoint < len(std_index)-1:
                std_set = set(std_index[:dampoint+1])
            else:
                std_set = set(std_index[:])
            opr_set = set(indexs)
            diff_set = std_set - opr_set
            if diff_set:
                missing_set.update(diff_set)
                report(missing_set,REPORT_PATH+'missing.txt')
        else:
            os.rename(REPORT_PATH+'missing.txt',
                      REPORT_PATH+'missing'+last_pre_folds+'.txt')
            last_pre_fold = last_fold
        time.sleep(5)


if __name__ == '__main__':
    main()
