#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Python 3.7

# @Time   : 2023/3/2 6:28
# @Author : 'Lou Zehua'
# @File   : time_format.py

import os
import sys

if '__file__' not in globals():
    # !pip install ipynbname  # Remove comment symbols to solve the ModuleNotFoundError
    import ipynbname

    nb_path = ipynbname.path()
    __file__ = str(nb_path)
cur_dir = os.path.dirname(__file__)
pkg_rootdir = os.path.dirname(cur_dir)  # os.path.dirname()向上一级，注意要对应工程root路径
if pkg_rootdir not in sys.path:  # 解决ipynb引用上层路径中的模块时的ModuleNotFoundError问题
    sys.path.append(pkg_rootdir)
    print('-- Add root directory "{}" to system path.'.format(pkg_rootdir))

from datetime import datetime
from dateutil.relativedelta import relativedelta


class TimeFormat:
    def __init__(self, date_string, input_format="%Y%m", output_format=None):
        self.default_input_format = input_format
        self.default_output_format = output_format or self.default_input_format
        self.reg_time = datetime.strptime(date_string, self.default_input_format)

    def get_last_month(self, output_format=None, n=1):
        output_format = output_format or self.default_output_format
        month_date = self.reg_time.date() - relativedelta(months=n)
        return month_date.strftime(output_format)

    def get_curr_month(self, output_format=None):
        output_format = output_format or self.default_output_format
        return self.reg_time.strftime(output_format)

    def get_next_month(self, output_format=None, n=1):
        output_format = output_format or self.default_output_format
        month_date = self.reg_time.date() + relativedelta(months=n)
        return month_date.strftime(output_format)


if __name__ == '__main__':
    t = TimeFormat("202301")
    print("Registered Time:", t.reg_time)
    print("========format strings=========")
    print(f"default_input_format: {t.default_input_format}, default_output_format: {t.default_output_format}")
    print(t.get_curr_month(), t.get_curr_month("%Y-%m"), t.get_curr_month("%b-%Y"))
    print(t.get_last_month("%b-%Y", 2))
    print(t.get_next_month("%b-%Y"))
