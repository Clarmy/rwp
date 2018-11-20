# coding:utf-8
'''
--------------------------------------------------------------------
项目名：rwp
模块名：algom.errors
模块错误对象
--------------------------------------------------------------------
python = 3.6
--------------------------------------------------------------------
'''
class OutputError(Exception):
    '''输出错误'''
    def __init__(self, message):
        self.message = message


class InputError(Exception):
    '''输入错误'''
    def __init__(self, message):
        self.message = message
