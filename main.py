#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Python 3.7

# @Time   : 2022/11/27 3:39
# @Author : 'Lou Zehua'
# @File   : main.py 

import os
import sys


cur_dir = os.getcwd()
pkg_rootdir = cur_dir  # os.path.dirname()向上一级，注意要对应工程root路径
if pkg_rootdir not in sys.path:
    sys.path.append(pkg_rootdir)
print('--Add root directory "{}" to system path.'.format(pkg_rootdir))


import shutil
import socket
import pandas as pd

from script.crawling_ranking_table import crawling_ranking_table_soup
from script.crawling_dbms_info import crawling_dbms_infos_soup
from script.join_ranking_table_dbms_info import join_ranking_table_dbms_info
from script.recalc_ranking_table_dbms_info import recalc_ranking_table_dbms_info
from script.reuse_existing_tagging_info import merge_info_to_csv, trim_spaces
from script.time_format import TimeFormat


UPDATE_RANKING_TABLE = False  # This will take a long time to crawl the DB-Engines website if set to True...
UPDATE_DBMS_INFO = False  # This will take a long long time to crawl many DB-Engines websites if set to True......
JOIN_RANKING_TABLE_DBMS_INFO_ON_DBMS = True  # join ranking_table and dbms_info on filed 'DBMS' and 'Name'
RECALC_RANKING_TABLE_DBMS_INFO = True
REUSE_EXISTING_TAGGING_INFO = True

format_time_in_filename = "%Y%m"
format_time_in_colname = "%b-%Y"

month_yyyyMM = "202303"
curr_month = TimeFormat(month_yyyyMM, format_time_in_filename, format_time_in_filename)


last_month_yyyyMM = curr_month.get_last_month()
src_existing_tagging_info_path = os.path.join(pkg_rootdir, f'data/manulabeled/ranking_crawling_{last_month_yyyyMM}_automerged_manulabled.csv')
ranking_table_crawling_path = os.path.join(pkg_rootdir, f'data/db_engines_ranking_table_full/ranking_crawling_{month_yyyyMM}_raw.csv')
dbms_info_crawling_path = os.path.join(pkg_rootdir, f'data/db_engines_ranking_table_full/dbms_info_crawling_{month_yyyyMM}_raw.csv')
ranking_table_dbms_info_joined_path = os.path.join(pkg_rootdir, f'data/db_engines_ranking_table_full/ranking_table_dbms_info_{month_yyyyMM}_joined.csv')
src_ranking_table_dbms_info_joined_recalc_path = os.path.join(pkg_rootdir, f'data/db_engines_ranking_table_full/ranking_table_dbms_info_{month_yyyyMM}_joined_recalc.csv')
tar_automerged_path = os.path.join(pkg_rootdir, f'data/db_engines_ranking_table_full/ranking_crawling_{month_yyyyMM}_automerged.csv')
src_category_labels_path = os.path.join(pkg_rootdir, f'data/existing_tagging_info/category_labels.csv')
tar_category_labels_updated_path = os.path.join(pkg_rootdir, f'data/db_engines_ranking_table_full/category_labels_updated.csv')

encoding = 'utf-8'

# headers info when use Chrome explorer
header1 = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/49.0.2623.221 Safari/537.36 SE 2.X MetaSr 1.0',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'
}
header2 = {
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.84 Safari/537.36'}
header3 = {
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36'}
header4 = {
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36'}
header5 = {
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36'}
header6 = {
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36'}
headers = [header1, header2, header3, header4, header5, header6]

if __name__ == '__main__':

    if UPDATE_RANKING_TABLE:
        url_init = "https://db-engines.com/en/ranking"
        use_elem_dict = {
            'main_contents': ['table', {'class': 'dbi'}],
        }
        crawling_ranking_table_soup(url_init, headers[0], use_elem_dict, save_path=ranking_table_crawling_path)

    if UPDATE_DBMS_INFO:
        df_ranking_table = pd.read_csv(ranking_table_crawling_path, encoding=encoding, index_col=False)
        # db-engines DBMS_insitelink
        df_db_names_urls = df_ranking_table[['DBMS', 'DBMS_insitelink']]
        df_db_names_urls.columns = ['db_names', 'urls']

        use_elem_dict = {
            'main_contents': ['table', {'class': 'tools'}],
        }
        mode = 'a'  # mode 'a' for breakpoint resumption
        batch = 20
        temp_save_path = dbms_info_crawling_path.rstrip('.csv') + '_temp.csv'
        state = -1
        while state == -1:
            try:
                state = crawling_dbms_infos_soup(df_db_names_urls, headers, use_elem_dict, save_path=dbms_info_crawling_path, mode=mode, temp_save_path=temp_save_path, batch=batch)
            except socket.timeout:
                continue

    if JOIN_RANKING_TABLE_DBMS_INFO_ON_DBMS:
        use_cols_ranking_table = None
        use_cols_dbms_infos = ["Developer", "Name", "Description", "Initial release", "Current release", "License", "License_info",
                               "Cloud-based only"]
        df_ranking_table = pd.read_csv(ranking_table_crawling_path, encoding=encoding, index_col=False)
        df_dbms_infos = pd.read_csv(dbms_info_crawling_path, encoding=encoding, index_col=False)
        join_ranking_table_dbms_info(df_ranking_table, df_dbms_infos, use_cols_ranking_table, use_cols_dbms_infos,
                                     save_path=ranking_table_dbms_info_joined_path, on_pair=("DBMS", "Name"))

    if RECALC_RANKING_TABLE_DBMS_INFO:
        df_ranking_table_dbms_info = pd.read_csv(ranking_table_dbms_info_joined_path, encoding=encoding, index_col=False)
        recalc_cols = ["Initial release", "Current release", "License", "Cloud-based only"]
        recalc_ranking_table_dbms_info(df_ranking_table_dbms_info, recalc_cols, save_path=src_ranking_table_dbms_info_joined_recalc_path)

    if REUSE_EXISTING_TAGGING_INFO:
        OVERWRITE_CATEGORY_LABELS = True

        df_src_existing_tagging_info = pd.read_csv(src_existing_tagging_info_path, encoding=encoding, index_col=0)
        df_ranking_table_dbms_info_joined_recalc = pd.read_csv(src_ranking_table_dbms_info_joined_recalc_path, encoding=encoding, index_col=False)

        try:
            df_category_labels = pd.read_csv(src_category_labels_path, encoding=encoding, index_col=False)
        except FileNotFoundError:

            from io import StringIO

            category_labels = '''
            id	category_name	category_label
            1	Relational DBMS	Relational DBMS
            2	Key-value stores	Key-value
            3	Document stores	Document
            4	Time Series DBMS	Time Series
            5	Graph DBMS	Graph
            6	Object oriented DBMS	Object oriented
            7	Search engines	Search engine
            8	RDF stores	RDF
            9	Wide column stores	Wide column
            10	Multivalue DBMS	Multivalue
            11	Native XML DBMS	Native XML
            12	Spatial DBMS	Spatial DBMS
            13	Event Stores	Event
            14	Content stores	Content
            15	Navigational DBMS	Navigational
            '''
            df_category_labels = pd.read_table(StringIO(category_labels), sep='\t', header='infer', index_col=0)

        # 更新设置
        update_conf = {
            'category_label': 'update__use_new(Database Model)',
            # update values and change the column name
            'Multi_model_info': 'update__use_new(Multi_model_info)',
            'DBMS': 'update__use_new',  # update values and change the column name
            'DBMS_insitelink': 'update__use_new',  # insert values
            'has_open_source_github_repo': 'update__reuse_old_if_cooccurrence_on(DBMS)',
            'has_company': 'update__reuse_old_if_cooccurrence_on(DBMS)',
            'github_repo_link': 'update__reuse_old_if_cooccurrence_on(DBMS)',
            # update values and change the column name
            f'Score_{curr_month.get_last_month(format_time_in_colname)}': f'update__change_colname_as(Score_{curr_month.get_curr_month(format_time_in_colname)})__use_new(Score_{curr_month.get_curr_month(format_time_in_colname)})',  # automatically updated with the variable "month_yyyyMM"
            f'Rank_{curr_month.get_last_month(format_time_in_colname)}': f'update__change_colname_as(Rank_{curr_month.get_curr_month(format_time_in_colname)})__use_new(Rank_{curr_month.get_curr_month(format_time_in_colname)})__dtype(Int64)',  # automatically updated with the variable "month_yyyyMM"
            'org_name': 'update__reuse_old_if_cooccurrence_on(DBMS)',  # 依赖于手动更新的列github_repo_link
            'repo_name': 'update__reuse_old_if_cooccurrence_on(DBMS)',  # 依赖于手动更新的列github_repo_link
            'Developer': 'update__reuse_old_if_cooccurrence_on(DBMS)',
            'initial_release_recalc': 'update__use_new__dtype(Int64)',
            'current_release_recalc': 'update__use_new__dtype(Int64)',
            'open_source_license': 'update__use_new(license_recalc)',
            'License_info': 'update__use_new',
            'cloud_based_only_recalc': 'update__use_new',
        }

        df_src_existing_tagging_info["DBMS"] = df_src_existing_tagging_info["DBMS"].apply(trim_spaces)
        df_ranking_table_dbms_info_joined_recalc["DBMS"] = df_ranking_table_dbms_info_joined_recalc["DBMS"].apply(trim_spaces)
        merge_info_to_csv(df_src_existing_tagging_info, df_ranking_table_dbms_info_joined_recalc, df_category_labels, update_conf,
                          save_automerged_path=tar_automerged_path,
                          save_category_labels_path=tar_category_labels_updated_path)

        if OVERWRITE_CATEGORY_LABELS:
            shutil.copy(tar_category_labels_updated_path, src_category_labels_path)
