#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Python 3.7

# @Time   : 2022/11/27 8:55
# @Author : 'Lou Zehua'
# @File   : recalc_ranking_table_dbms_info.py 

import os
import sys

cur_dir = os.getcwd()
pkg_rootdir = os.path.dirname(cur_dir)  # os.path.dirname()向上一级，注意要对应工程root路径
if pkg_rootdir not in sys.path:
    sys.path.append(pkg_rootdir)
print('Add root directory "{}" to system path.'.format(pkg_rootdir))

import re

import pandas as pd


encoding = 'utf-8'


class RecalcFuncPool(object):

    @staticmethod
    def developer_recalc_func(series):
        res_series = pd.Series(series, dtype=str)
        return res_series

    @staticmethod
    def initial_release_recalc_func(series):
        res_series = pd.Series(series, dtype=int)
        res_series = res_series.apply(lambda x: int(x) if pd.notna(x) else '')
        res_series = pd.Series(res_series, dtype=int)
        return res_series

    @staticmethod
    def current_release_recalc_func(series):
        res_series = pd.Series(series, dtype=str)
        def get_year(s):
            year_strs = re.findall(r'\d{4}', s)
            year = year_strs[-1] if len(year_strs) else ''
            return year

        res_series = res_series.apply(get_year)
        res_series = pd.Series(res_series, dtype=int)
        return res_series

    @staticmethod
    def license_recalc_func(series):
        res_series = pd.Series(series, dtype=str)
        res_series = res_series.apply(str.lower)
        res_series[res_series == "open source"] = 'Y'
        res_series[res_series == "commercial"] = 'N'
        return res_series

    @staticmethod
    def cloud_based_only_recalc_func(series):
        res_series = pd.Series(series, dtype=str)
        res_series = res_series.apply(str.lower)
        res_series[res_series == 'yes'] = 'Y'
        res_series[res_series == 'no'] = 'N'
        return res_series


def recalc_ranking_table_dbms_info(df_ranking_table_dbms_info, recalc_cols, save_path, **kwargs):
    columns = df_ranking_table_dbms_info.columns
    for col in recalc_cols:
        if col in columns:
            recalc_colname = col.lower().replace(' ', '_').replace('-', '_') + "_recalc"
            recalc_func_default_str = col.lower().replace(' ', '_').replace('-', '_') + "_recalc_func"
            if recalc_colname in kwargs.keys():
                df_ranking_table_dbms_info[recalc_colname] = kwargs[recalc_colname](df_ranking_table_dbms_info[col])
            else:
                df_ranking_table_dbms_info[recalc_colname] = getattr(RecalcFuncPool, recalc_func_default_str)(df_ranking_table_dbms_info[col])
        else:
            print(f"{col} not in the columns of input dataframe! ignored!")
    df_ranking_table_dbms_info.to_csv(save_path, encoding=encoding, index=False)
    print(save_path, 'saved!')
    return None


if __name__ == '__main__':
    ranking_table_dbms_info_joined_path = os.path.join(pkg_rootdir, 'data/db_engines_ranking_table_full/ranking_table_dbms_info_202211_joined.csv')
    src_ranking_table_dbms_info_joined_recalc_path = os.path.join(pkg_rootdir, 'data/db_engines_ranking_table_full/ranking_table_dbms_info_202211_joined_recalc.csv')

    df_ranking_table_dbms_info = pd.read_csv(ranking_table_dbms_info_joined_path, encoding=encoding, index_col=False)
    recalc_cols = ["Initial release", "Current release", "License", "Cloud-based only"]
    recalc_ranking_table_dbms_info(df_ranking_table_dbms_info, recalc_cols,
                                   save_path=src_ranking_table_dbms_info_joined_recalc_path)
