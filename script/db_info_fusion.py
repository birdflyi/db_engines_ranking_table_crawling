#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Python 3.9

# @Time   : 2023/3/30 4:27
# @Author : 'Lou Zehua'
# @File   : db_info_fusion.py

import os
import sys

import pandas as pd

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

import copy

import numpy as np


def merge_info_start_checkpoint_last_month_manulabeled(df1, df2, save_path, input_key_colname_pair, output_key_colname,
                                                       use_columns_merged=None, conflict_delimiter="#df1>|<df2#", encoding="utf-8"):
    df1 = pd.DataFrame(df1)
    df2 = pd.DataFrame(df2)
    columns1 = list(df1.columns.values)
    columns2 = list(df2.columns.values)
    merge_columns = columns1 + [i for i in columns2 if i not in columns1]
    key_df1, key_df2 = tuple(input_key_colname_pair[:2])
    try:
        merge_columns.remove(key_df1)
        merge_columns.remove(key_df2)
    except ValueError:
        pass

    if output_key_colname not in merge_columns:
        merge_columns = [output_key_colname] + merge_columns
    use_columns_merged = use_columns_merged or merge_columns

    df1_key_col = df1[[key_df1]].reset_index().set_index(key_df1).astype(str)
    df2_key_col = df2[[key_df2]].reset_index().set_index(key_df2).astype(str)
    lsuffix = "_df1"
    rsuffix = "_df2"
    df_db_name_mapping = df1_key_col.join(df2_key_col, how="left", lsuffix=lsuffix, rsuffix=rsuffix)

    df_res = copy.deepcopy(df_db_name_mapping)
    index_df1_suffixed = ""
    index_df2_suffixed = ""
    for c in merge_columns:
        key_column_on_process = c == output_key_colname
        if key_column_on_process:
            df1.set_index(key_df1, inplace=True)
            df2.set_index(key_df2, inplace=True)
            index_df1_suffixed = "index" + lsuffix
            index_df2_suffixed = "index" + rsuffix
            continue
        df1_has_cur_col = c in df1.columns
        df2_has_cur_col = c in df2.columns
        if not df1_has_cur_col and not df2_has_cur_col:
            raise ValueError(
                f"At least one of the input columns is not None! Unknown error on column {c}!")
        elif df1_has_cur_col and not df2_has_cur_col:
            temp_values = []
            temp_df1_series = df_db_name_mapping[index_df1_suffixed]
            for k_rec in range(len(df_db_name_mapping)):
                try:
                    temp_rec_df1_index = temp_df1_series[k_rec]
                    if pd.notna(temp_rec_df1_index):
                        temp_rec_df1_index = int(temp_rec_df1_index)
                        temp_item_df1 = df1.iloc[temp_rec_df1_index][c]
                    else:
                        temp_item_df1 = np.nan
                except KeyError:
                    temp_item_df1 = np.nan
                temp_values.append(temp_item_df1)
            df_res[c] = temp_values
        elif not df1_has_cur_col and df2_has_cur_col:
            temp_values = []
            temp_df2_series = df_db_name_mapping[index_df2_suffixed]
            for k_rec in range(len(df_db_name_mapping)):
                try:
                    temp_rec_df2_index = temp_df2_series[k_rec]
                    if pd.notna(temp_rec_df2_index):
                        temp_rec_df2_index = int(temp_rec_df2_index)
                        temp_item_df2 = df2.iloc[temp_rec_df2_index][c]
                    else:
                        temp_item_df2 = np.nan
                except KeyError:
                    temp_item_df2 = np.nan
                temp_values.append(temp_item_df2)
            df_res[c] = temp_values
        else:
            temp_values = []
            temp_df1_series = df_db_name_mapping[index_df1_suffixed]
            temp_df2_series = df_db_name_mapping[index_df2_suffixed]
            for k_rec in range(len(df_db_name_mapping)):
                try:
                    temp_rec_df1_index = temp_df1_series[k_rec]
                    if pd.notna(temp_rec_df1_index):
                        temp_rec_df1_index = int(temp_rec_df1_index)
                        temp_item_df1 = df1.iloc[temp_rec_df1_index][c]
                    else:
                        temp_item_df1 = np.nan
                except KeyError:
                    temp_item_df1 = np.nan
                try:
                    temp_rec_df2_index = temp_df2_series[k_rec]
                    if pd.notna(temp_rec_df2_index):
                        temp_rec_df2_index = int(temp_rec_df2_index)
                        temp_item_df2 = df2.iloc[temp_rec_df2_index][c]
                    else:
                        temp_item_df2 = np.nan
                except KeyError:
                    temp_item_df2 = np.nan
                if pd.notna(temp_item_df1) and pd.notna(temp_item_df2):
                    if temp_item_df1 == temp_item_df2:
                        v = temp_item_df1
                    else:
                        v = str(temp_item_df1) + conflict_delimiter + str(temp_item_df2)
                else:
                    v = temp_item_df1 if pd.notna(temp_item_df1) else temp_item_df2
                temp_values.append(v)
            df_res[c] = temp_values

    df_res.index.name = output_key_colname
    df_res.reset_index(inplace=True)
    df_res = df_res[use_columns_merged]
    df_res.to_csv(save_path, encoding=encoding, index=False)
    print(save_path, 'saved!')
    return
