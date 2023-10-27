#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Python 3.7

# @Time   : 2022/11/20 22:30
# @Author : 'Lou Zehua'
# @File   : crawling_ranking_table.py 

import os
import sys

cur_dir = os.getcwd()
pkg_rootdir = os.path.dirname(cur_dir)  # os.path.dirname()向上一级，注意要对应工程root路径
if pkg_rootdir not in sys.path:
    sys.path.append(pkg_rootdir)
print('Add root directory "{}" to system path.'.format(pkg_rootdir))


import urllib

import pandas as pd
import bs4

from bs4 import BeautifulSoup
from urllib import request, parse


def crawling_ranking_table_soup(url_init, header, use_elem_dict, save_path):
    request = urllib.request.Request(url_init, headers=header)
    response = urllib.request.urlopen(request)
    # print(response.read().decode("utf-8"))
    soup = BeautifulSoup(response, 'lxml')  # 利用bs4库解析html
    # soup = BeautifulSoup(s, 'lxml')  # 利用bs4库解析html
    response.close()  # 注意关闭response

    # 取出主内容
    main_contents = soup.find_all(use_elem_dict['main_contents'][0], attrs=use_elem_dict['main_contents'][1])
    ranking_table = main_contents[0]

    # 获取所需文本
    trs = ranking_table.find_all('tr')

    # 1. get dbi_description
    dbi_description_elem = trs[0]
    dbi_description = dbi_description_elem.find('td').text
    print(dbi_description)

    # 2. get dbi_header_cols
    def th_td_filter_func(th_td_tag):
        return ((th_td_tag.name == 'th' and th_td_tag.parent.name == 'tr') or
                (th_td_tag.name == 'td' and th_td_tag.parent.name == 'tr'))

    dbi_header_elem = trs[1]
    dbi_header_small_elem = trs[2]

    # a. dbi_header_cols prefix
    dbi_header_prefix_elems = dbi_header_elem.find_all(th_td_filter_func)
    dbi_header_prefix_config = []
    for dbi_header_prefix_elem in dbi_header_prefix_elems:
        suffix_prepare_num = 0
        if 'colspan' in dbi_header_prefix_elem.attrs:
            suffix_prepare_num = int(dbi_header_prefix_elem.attrs["colspan"])
        dbi_header_prefix_text = dbi_header_prefix_elem.text
        dbi_header_prefix_config.append([dbi_header_prefix_text, suffix_prepare_num])
    need_suffix_nums = [prefix_pair[1] for prefix_pair in dbi_header_prefix_config]
    need_suffix_num_sum = sum(need_suffix_nums)

    # b. dbi_header_cols suffix
    dbi_header_suffix_elems = dbi_header_small_elem.find_all('td')
    assert(len(dbi_header_suffix_elems) == need_suffix_num_sum)

    dbi_header_cols = []
    idx_suffix = 0
    for i in range(len(dbi_header_prefix_config)):
        dbi_header_prefix_text, suffix_prepare_num = dbi_header_prefix_config[i]
        if suffix_prepare_num:
            for _ in range(suffix_prepare_num):
                dbi_header_col = dbi_header_prefix_text + '_' + dbi_header_suffix_elems[idx_suffix].get_text('-', '<br/>')
                dbi_header_cols.append(dbi_header_col)
                idx_suffix += 1
        else:
            dbi_header_cols.append(dbi_header_prefix_text)
    dbi_header_cols.append('DBMS_insitelink')
    dbi_header_cols.append('Multi_model_info')
    # print(dbi_header_cols)

    # 3. get dbi_body
    dbi_body_elems = trs[3:]
    len_dbi_body_elems = len(dbi_body_elems)
    if str(len_dbi_body_elems) not in dbi_description:
        print("Error: Wrong length of table body, get len_dbi_body_elems = {len_dbi_body_elems}, while it cannot be found "
              "in dbi_description: '{dbi_description}'!".format(len_dbi_body_elems=len_dbi_body_elems, dbi_description=dbi_description))

    dbi_body_records = []
    for dbi_body_elem in dbi_body_elems:
        record_elems = dbi_body_elem.find_all(th_td_filter_func)
        record_items = []
        DBMS_insitelink = ''
        multi_model_info = ''
        USE_FIRST_MATCHED_SPAN__MULTI_MODEL_INFO = True
        set_flag__multi_model_info = False
        for record_elem in record_elems:
            if record_elem.name == 'th' and "class" in record_elem.attrs:
                s_extracts_span = [s.extract() for s in record_elem(['span'])]  # must be executed before get record_elem.text
                record_item = record_elem.text.strip()  # record_elem has been extracted multi_model tags

                if record_elem.attrs["class"] == ["pad-l"]:
                    # get DBMS_insitelink when record_elem = '''
                    # <th class="pad-l">
                    # 	<a href="https://db-engines.com/en/system/Oracle">
                    # 		Oracle
                    # 		<span class="info"><img /></span>
                    # 		<span class="infobox infobox_r"></span>
                    # 	</a>
                    # </th>
                    s_extracts_a = [s.extract() for s in record_elem(['a'])]
                    USE_FIRST_MATCHED_A = True
                    pos = 0 if USE_FIRST_MATCHED_A else -1  # use the first matched a or the last matched a.
                    s_extract_a = s_extracts_a[pos]
                    DBMS_insitelink = s_extract_a.attrs['href'].strip()

                elif record_elem.attrs["class"] == ["small", "pad-r"]:
                    # update multi_model_info when record_elem = '''
                    # <th class="small pad-r">
                    #   <span class="info">
                    #       <span class="infobox infobox_r"></span>
                    #   </span>
                    # </th>
                    # '''
                    for s_extract_span in s_extracts_span:
                        if "class" in s_extract_span.attrs:
                            if s_extract_span.attrs["class"] == ["infobox", "infobox_r"]:
                                multi_model_extract_span = s_extract_span
                                update_flag = not (USE_FIRST_MATCHED_SPAN__MULTI_MODEL_INFO and set_flag__multi_model_info)
                                if update_flag:
                                    multi_model_info = multi_model_extract_span.get_text(',', '<br/>').strip()
                                    set_flag__multi_model_info = True
            else:
                record_item = record_elem.text.strip()

            record_items.append(record_item)
        record_items.append(DBMS_insitelink)
        record_items.append(multi_model_info)
        dbi_body_records.append(record_items)
    # print(dbi_body_records)

    # 4. save to csv
    df = pd.DataFrame(dbi_body_records, columns=dbi_header_cols)
    df.to_csv(save_path, encoding='utf-8', index=False)
    print(save_path, 'saved!')
    return None


if __name__ == '__main__':
    # db-engines http link
    url_init = "https://db-engines.com/en/ranking"
    # headers info when use Chrome explorer
    header = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/49.0.2623.221 Safari/537.36 SE 2.X MetaSr 1.0',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'
        }
    use_elem_dict = {
        'main_contents': ['table', {'class': 'dbi'}],
    }
    ranking_table_crawling_path = os.path.join(pkg_rootdir, 'data/db_engines_ranking_table_full/ranking_crawling_202211_raw.csv')
    crawling_ranking_table_soup(url_init, header, use_elem_dict, ranking_table_crawling_path)
