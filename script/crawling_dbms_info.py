#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Python 3.7

# @Time   : 2022/11/25 3:54
# @Author : 'Lou Zehua'
# @File   : crawling_dbms_info.py 

import os
import sys

cur_dir = os.getcwd()
pkg_rootdir = os.path.dirname(cur_dir)  # os.path.dirname()向上一级，注意要对应工程root路径
if pkg_rootdir not in sys.path:
    sys.path.append(pkg_rootdir)
print('Add root directory "{}" to system path.'.format(pkg_rootdir))

import urllib

import pandas as pd

from bs4 import BeautifulSoup
from urllib import request


def crawling_dbms_info_soup(url_init, header, use_elem_dict):
    request = urllib.request.Request(url_init, headers=header)
    response = urllib.request.urlopen(request)
    response_body = response.read().decode('utf-8').replace('&shy;', '')
    soup = BeautifulSoup(response_body, 'lxml')  # 利用bs4库解析html
    response.close()  # 注意关闭response

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
                        dbms_info_record_attrs_dict[attr_key] = ''
                        # print(i, "class:", attr_key)
                    if class_value in ["value", "header"] and attr_key:
                        attr_value = th_td_tag.get_text(',', '<br/>').strip()
                        dbms_info_record_attrs_dict[attr_key] = attr_value
                        # print(i, "value:", attr_value)
    return dbms_info_record_attrs_dict


def crawling_dbms_infos_soup(df_db_names_urls, header, use_elem_dict, save_path, use_cols=None):
    try:
        df_db_names_urls = pd.DataFrame(df_db_names_urls)[["db_names", "urls"]]
    except:
        if type(df_db_names_urls) == dict:
            df_db_names_urls = pd.DataFrame(df_db_names_urls.items(), columns=["db_names", "urls"])
    # print(db_names_urls)

    default_use_cols = ['Name', 'Description', 'Primary database model', 'Secondary database models',
       'DB-Engines Ranking Trend Chart', 'Website', 'Technical documentation',
       'Developer', 'Initial release', 'Current release', 'License',
       'Cloud-based only', 'DBaaS offerings (sponsored links)',
       'Implementation language', 'Server operating systems', 'Data scheme',
       'Typing', 'XML support', 'Secondary indexes', 'SQL',
       'APIs and other access methods', 'Supported programming languages',
       'Server-side scripts', 'Triggers', 'Partitioning methods',
       'Replication methods', 'MapReduce', 'Consistency concepts',
       'Foreign keys', 'Transaction concepts', 'Concurrency', 'Durability',
       'In-memory capabilities', 'User concepts']
    use_cols = use_cols or default_use_cols
    if "Name" not in use_cols:
        use_cols = ["Name"] + use_cols

    df_dbms_infos = pd.DataFrame()

    len_db_names = len(df_db_names_urls)
    for i in range(len_db_names):
        db_name, url = df_db_names_urls.iloc[i]
        print(f"{i}/{len_db_names}: Crawling data for {db_name} on {url} ...")
        dbms_info_record_attrs_dict = crawling_dbms_info_soup(url, header, use_elem_dict)
        crawling_db_name = dbms_info_record_attrs_dict["Name"]
        if db_name == crawling_db_name:
            series_dbms_info = pd.Series(data=None, index=use_cols, dtype=str)
            series_dbms_info.update(pd.Series(dbms_info_record_attrs_dict))
            series_dbms_info = series_dbms_info[use_cols]
            df_dbms_infos[db_name] = series_dbms_info
        else:
            print(f"Unmatched dbms name, expect {db_name} but get {crawling_db_name} please check the website: {url}")

    df_dbms_infos = df_dbms_infos.T
    # print(df_dbms_infos)

    # save to csv
    df_dbms_infos.to_csv(save_path, encoding='utf-8', index=False)
    print(save_path, 'saved!')
    return None


if __name__ == '__main__':
    ranking_crawling_path = os.path.join(pkg_rootdir, 'data/db_engines_ranking_table_full/ranking_crawling_202211_raw.csv')
    dbms_info_crawling_path = os.path.join(pkg_rootdir, 'data/db_engines_ranking_table_full/dbms_info_crawling_202211_raw.csv')

    df_ranking_crawling = pd.read_csv(ranking_crawling_path, encoding='utf-8', index_col=False)
    # db-engines DBMS_insitelink
    df_db_names_urls = df_ranking_crawling[['DBMS', 'DBMS_insitelink']]
    df_db_names_urls.columns = ['db_names', 'urls']

    # 在浏览器下获取他们的headers信息
    header = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/49.0.2623.221 Safari/537.36 SE 2.X MetaSr 1.0',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'
        }
    use_elem_dict = {
        'main_contents': ['table', {'class': 'tools'}],
    }
    crawling_dbms_infos_soup(df_db_names_urls, header, use_elem_dict, dbms_info_crawling_path)
