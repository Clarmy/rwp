'''
--------------------------------------------------------------------
项目名：rwp
模块名：opr.ispt
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
from opr.optools import check_dir, get_today_date
from opr.optools import standard_time_index

with open('../config.json') as f:
    config = js.load(f)

ROOT_PATH = config['parse']['oper']['save_path']
REPORT_PATH = '/mnt/data14/liwt/opr/parse/missing/'
check_dir(REPORT_PATH)

def report(missing_set,pfn):
    '''生成缺失文件报告'''
    now = datetime.utcnow().strftime('%Y%m%d%H%M%S')
    content = ['record\'s utc time:\n\n    {}\n\n'.format(now),
               'missing files are as follow:\n\n']
    missing_list = list(missing_set)
    missing_list.sort()
    for m in missing_list:
        content.append('    {}\n'.format(m))
    content.append('\n')

    with open(pfn,'w') as f:
        f.writelines(content)

    return True


def main():
    missing_set = set([])
    while True:

        std_index = standard_time_index()
        today = get_today_date()

        folds = sorted(os.listdir(ROOT_PATH))
        last_fold = folds[-1]
        path = ROOT_PATH + last_fold + '/'

        filenames = os.listdir(path)
        indexs = sorted([fn.split('.')[0] for fn in filenames])
        last_time = indexs[-1]

        dampoint = std_index.index(last_time)
        if dampoint < len(std_index)-1:
            std_set = set(std_index[:dampoint+1])
        else:
            std_set = set(std_index)
        opr_set = set(indexs)
        diff_set = std_set - opr_set
        if diff_set:
            missing_set.update(diff_set)
            report(missing_set,REPORT_PATH+'%s.txt' % today)

        time.sleep(5)


if __name__ == '__main__':
    main()
