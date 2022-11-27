#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Python 3.7

# @Time   : 2022/11/27 9:02
# @Author : 'Lou Zehua'
# @File   : join_ranking_table_dbms_info.py 

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


def join_ranking_table_dbms_info(df_ranking_table, df_dbms_infos, use_cols_ranking_table, use_cols_dbms_infos, save_path,
                                 on_pair=None, key_alias=None):
    on_pair = on_pair or ("DBMS", "Name")
    on_df_ranking_table = on_pair[0]
    on_df_dbms_infos = on_pair[1]
    key_alias = key_alias or on_pair[0]
    use_cols_ranking_table = use_cols_ranking_table or list(df_ranking_table.columns)
    if on_df_ranking_table not in use_cols_ranking_table:
        use_cols_ranking_table = [on_df_ranking_table] + use_cols_ranking_table
    use_cols_dbms_infos = use_cols_dbms_infos or list(df_dbms_infos.columns)
    if on_df_dbms_infos not in use_cols_dbms_infos:
        use_cols_dbms_infos = [on_df_dbms_infos] + use_cols_dbms_infos
    df_ranking_table_filtered = df_ranking_table[use_cols_ranking_table]
    df_dbms_infos_filtered = df_dbms_infos[use_cols_dbms_infos]
    df_ranking_table_filtered.set_index(on_df_ranking_table, inplace=True)
    df_ranking_table_filtered.index.name = key_alias  # change index name from "Name" to "DBMS"
    df_dbms_infos_filtered.set_index(on_df_dbms_infos, inplace=True)
    df_dbms_infos_filtered.index.name = key_alias  # change index name from "Name" to "DBMS"
    df_ranking_table_dbms_info_joined = df_ranking_table_filtered.join(df_dbms_infos_filtered, on=key_alias)
    df_ranking_table_dbms_info_joined.reset_index(inplace=True)
    # pd.set_option("display.max_columns", None)
    # print(df_ranking_table_dbms_info_joined)
    # save to csv
    df_ranking_table_dbms_info_joined.to_csv(save_path, encoding=encoding, index=False)
    print(save_path, 'saved!')


if __name__ == '__main__':
    ranking_table_crawling_path = os.path.join(pkg_rootdir, 'data/db_engines_ranking_table_full/ranking_crawling_202211_raw.csv')
    dbms_info_crawling_path = os.path.join(pkg_rootdir, 'data/db_engines_ranking_table_full/dbms_info_crawling_202211_raw.csv')
    ranking_table_dbms_info_joined_path = os.path.join(pkg_rootdir, 'data/db_engines_ranking_table_full/ranking_table_dbms_info_202211_joined.csv')

    use_cols_ranking_table = None
    use_cols_dbms_infos = ["Developer", "Name", "Description", "Initial release", "Current release", "License", "Cloud-based only"]

    df_ranking_table = pd.read_csv(ranking_table_crawling_path, encoding=encoding, index_col=False)
    df_dbms_infos = pd.read_csv(dbms_info_crawling_path, encoding=encoding, index_col=False)
    join_ranking_table_dbms_info(df_ranking_table, df_dbms_infos, use_cols_ranking_table, use_cols_dbms_infos,
                                 save_path=ranking_table_dbms_info_joined_path, on_pair=("DBMS", "Name"))
