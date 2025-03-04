#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Python 3.7

# @Time   : 2022/11/25 3:54
# @Author : 'Lou Zehua'
# @File   : crawling_dbms_info.py 

import os
import shutil
import sys
import time

cur_dir = os.getcwd()
pkg_rootdir = os.path.dirname(cur_dir)  # os.path.dirname()向上一级，注意要对应工程root路径
if pkg_rootdir not in sys.path:
    sys.path.append(pkg_rootdir)
print('Add root directory "{}" to system path.'.format(pkg_rootdir))

import re
import urllib

import pandas as pd

from bs4 import BeautifulSoup
from urllib import request


STATE_OK = 0

def crawling_dbms_info_soup(url_init, header, use_elem_dict):
    request = urllib.request.Request(url_init, headers=header)
    response = urllib.request.urlopen(request, timeout=60*5)
    response_body = response.read().decode('utf-8').replace('&shy;', '')
    response.close()  # 注意关闭response
    soup = BeautifulSoup(response_body, 'lxml')  # 利用bs4库解析html

    # 取出主内容
    main_contents = soup.find_all(use_elem_dict['main_contents'][0], attrs=use_elem_dict['main_contents'][1])
    ranking_table = main_contents[0]

    # 获取所需文本
    trs = ranking_table.find_all('tr')

    def th_td_filter_func(th_td_tag):
        return ((th_td_tag.name == 'th' and th_td_tag.parent.name == 'tr') or
                (th_td_tag.name == 'td' and th_td_tag.parent.name == 'tr'))

    dbms_info_record_attrs_dict = {}
    for i, tr in enumerate(trs):
        attr_desc_flag = False  # 过滤出想要的属性
        th_td_tags = tr.find_all(th_td_filter_func)
        for th_td_tag in th_td_tags:
            if th_td_tag.name == 'td' and "class" in th_td_tag.attrs.keys():
                class_value = ' '.join(th_td_tag.attrs["class"])
                if class_value == 'attribute':  # 不包含"attribute external_att"
                    attr_desc_flag = True
                    break
        if attr_desc_flag:
            attr_key = None
            for th_td_tag in th_td_tags:
                if th_td_tag.name == 'td' and "class" in th_td_tag.attrs.keys():
                    s_extracts_span = [s.extract() for s in th_td_tag(['span'])]
                    class_value = ' '.join(th_td_tag.attrs["class"])
                    if class_value == 'attribute':  # 不包含"attribute external_att"
                        attr_key = th_td_tag.text.strip()
                        attr_key = re.sub(r' +', ' ', attr_key)
                        # dbms_info_record_attrs_dict[attr_key] = ''
                        # print(i, "class:", attr_key)
                    elif class_value in ["value", "header"] and attr_key:
                        attr_value = th_td_tag.get_text(',', '<br/>').strip()
                        attr_value = re.sub(r' +', ' ', attr_value)
                        dbms_info_record_attrs_dict[attr_key] = attr_value
                        if attr_key.lower() == "license":
                            license_info_value_parts = []
                            for s in s_extracts_span:
                                temp_text = s.get_text(',', '<br/>').strip()
                                temp_text = re.sub(r' +', ' ', temp_text)
                                if len(temp_text):
                                    license_info_value_parts.append(temp_text)
                            dbms_info_record_attrs_dict[attr_key + "_info"] = ';'.join(license_info_value_parts)
                        # print(i, "value:", attr_value)
    return dbms_info_record_attrs_dict


def crawling_dbms_infos_soup(df_db_names_urls, headers, use_elem_dict, save_path, use_cols=None, use_all_impl_cols=True, **kwargs):
    ADD_MODE = kwargs.get('mode', None) == 'a'
    temp_save_path = kwargs.get('temp_save_path', None)
    if ADD_MODE:
        print('Add mode...')
        if not temp_save_path:
            print('Error: add mode must specify a "temp_save_path" in arguments!')
            return

    try:
        df_db_names_urls = pd.DataFrame(df_db_names_urls)[["db_names", "urls"]]
    except:
        if type(df_db_names_urls) == dict:
            df_db_names_urls = pd.DataFrame(df_db_names_urls.items(), columns=["db_names", "urls"])
    # print(db_names_urls)

    KEY_ATTR_DBENG = 'Name'

    default_use_cols = [KEY_ATTR_DBENG, 'Description', 'Primary database model', 'Secondary database models',
                        'DB-Engines Ranking Trend Chart', 'Website', 'Technical documentation',
                        'Developer', 'Initial release', 'Current release', 'License', 'License_info',
                        'Cloud-based only', 'DBaaS offerings (sponsored links)',
                        'Implementation language', 'Server operating systems', 'Data scheme',
                        'Typing', 'XML support', 'Secondary indexes', 'SQL',
                        'APIs and other access methods', 'Supported programming languages',
                        'Server-side scripts', 'Triggers', 'Partitioning methods',
                        'Replication methods', 'MapReduce', 'Consistency concepts',
                        'Foreign keys', 'Transaction concepts', 'Concurrency', 'Durability',
                        'In-memory capabilities', 'User concepts']
    if use_all_impl_cols:
        use_cols = default_use_cols
        if use_cols:
            print("Warning: use_all_impl_cols=True will disable the parameter use_cols!")
    else:
        use_cols = use_cols or default_use_cols

    if KEY_ATTR_DBENG not in use_cols:
        use_cols = [KEY_ATTR_DBENG] + use_cols

    df_dbms_infos = pd.DataFrame(columns=use_cols)
    df_dbms_infos = df_dbms_infos.T

    len_db_names = len(df_db_names_urls)

    df1 = pd.DataFrame(columns=use_cols)
    batch = 100
    idx_start_end = [0, len_db_names]
    if ADD_MODE:
        encoding = kwargs.get('encoding', 'utf-8')
        index_col = kwargs.get('index_col', False)
        try:
            df1 = pd.read_csv(temp_save_path, encoding=encoding, index_col=index_col)
            df1 = df1[use_cols]
        except FileNotFoundError:
            df1.to_csv(temp_save_path, encoding='utf-8', index=False)
        except BaseException:
            pass

        len_df1 = len(df1)
        batch = kwargs.get('batch', batch)
        idx_start_end = kwargs.get('idx_start_end', [len_df1, min(len_df1 + batch, len_db_names)])

        assert(len_df1 == idx_start_end[0] and idx_start_end[0] <= idx_start_end[1])

    order_id_start_end = [idx_start_end[0] + 1, idx_start_end[1]]
    print('order_id_start_end:', order_id_start_end)
    for i in list(range(len_db_names))[idx_start_end[0]: idx_start_end[1]]:
        db_name, url = df_db_names_urls.iloc[i]
        print(f"{i + 1}/{len_db_names}: Crawling data for {db_name} on {url} ...")
        header = headers[i % len(headers)]
        dbms_info_record_attrs_dict = crawling_dbms_info_soup(url, header, use_elem_dict)
        if use_all_impl_cols:
            temp_use_cols = list(dbms_info_record_attrs_dict.keys())
            use_cols.extend(e for e in temp_use_cols if e not in use_cols)
        try:
            crawling_db_name = dbms_info_record_attrs_dict[KEY_ATTR_DBENG]
        except ValueError:
            print("DB-Engines website may changed the key attribute of DBMS system properties table! Please"
                  "update KEY_ATTR_DBENG!")
            return

        if db_name == crawling_db_name:
            series_dbms_info = pd.Series(data=None, index=use_cols, dtype=str)
            series_dbms_info.update(pd.Series(dbms_info_record_attrs_dict))
            series_dbms_info = series_dbms_info[use_cols]
            df_dbms_infos[db_name] = series_dbms_info
        else:
            print(f"Unmatched dbms name, expect {db_name} but get {crawling_db_name} please check the website: {url}")
        time.sleep(0.5)
        # break

    df_dbms_infos = df_dbms_infos.T
    # print(df_dbms_infos)

    if ADD_MODE:
        df2 = df_dbms_infos
        has_new_data = idx_start_end[1] < len_db_names or len(df2)
        if has_new_data:
            # save the data crawled in this batch
            join = kwargs.get('join', 'outer')
            df_dbms_infos_batched = pd.concat([df1, df2], join=join)
            df_dbms_infos_batched.to_csv(temp_save_path, encoding='utf-8', index=False)
            print(f"{temp_save_path} saved! idx_start_end:{idx_start_end}.")
            # Recursive crawling when this batch has new data
            new_idx_start_end = [idx_start_end[1], min(idx_start_end[1] + batch, len_db_names)]
            kwargs['idx_start_end'] = new_idx_start_end
            crawling_dbms_infos_soup(df_db_names_urls, headers, use_elem_dict, save_path, use_cols=use_cols,
                                     use_all_impl_cols=use_all_impl_cols, **kwargs)
        else:  # The exit of recursive crawling
            print(f'Index >= {len_db_names}, the crawling tasks has done! idx_start_end:{idx_start_end}.')
            # save to csv
            shutil.copyfile(temp_save_path, save_path)
            print(save_path, 'saved!')
            return STATE_OK
    else:
        # save to csv
        df_dbms_infos.to_csv(save_path, encoding='utf-8', index=False)
        print(save_path, 'saved!')
    return STATE_OK


if __name__ == '__main__':
    ranking_table_path = os.path.join(pkg_rootdir, 'data/db_engines_ranking_table_full/ranking_crawling_202211_raw.csv')
    dbms_info_crawling_path = os.path.join(pkg_rootdir, 'data/db_engines_ranking_table_full/dbms_info_crawling_202211_raw.csv')

    df_ranking_table = pd.read_csv(ranking_table_path, encoding='utf-8', index_col=False)
    # db-engines DBMS_insitelink
    df_db_names_urls = df_ranking_table[['DBMS', 'DBMS_insitelink']]
    df_db_names_urls.columns = ['db_names', 'urls']

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

    use_elem_dict = {
        'main_contents': ['table', {'class': 'tools'}],
    }
    crawling_dbms_infos_soup(df_db_names_urls, headers, use_elem_dict, dbms_info_crawling_path)
