#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Python 3.7

# @Time   : 2022/11/21 4:37
# @Author : 'Lou Zehua'
# @File   : reuse_existing_tagging_info.py

import os
import sys

cur_dir = os.getcwd()
pkg_rootdir = os.path.dirname(cur_dir)  # os.path.dirname()向上一级，注意要对应工程root路径
if pkg_rootdir not in sys.path:
    sys.path.append(pkg_rootdir)
print('Add root directory "{}" to system path.'.format(pkg_rootdir))

import re
import shutil
import numpy as np
import pandas as pd


encoding = 'utf-8'


def auto_gen_dbms_model_type_dict_by_keys(keys, values, ignore_keys=None,
                                          k_convert_dtype_func=None, v_convert_dtype_func=None,
                                          v2k_map_rule_func=str.startswith, keep='first', default_value='set_same_as_key'):
    if keep not in ['first', 'last']:
        print("Argument keep must be in ['first', 'last']! But got keep=", keep)
        return

    temp_default_value = ''
    dict_dbms_model_type = {}
    for k in keys:
        k = k if not k_convert_dtype_func else k_convert_dtype_func(k)

        k_in_ignore_keys = False
        if ignore_keys:
            if k in ignore_keys:
                k_in_ignore_keys = True

        if not k_in_ignore_keys:
            if not default_value:
                temp_default_value = ''
            elif type(default_value) == dict:
                if k in dict(default_value).keys():
                    temp_default_value = default_value[k]
                else:
                    temp_default_value = ''
            elif type(default_value) == str:
                if default_value == 'set_same_as_key':
                    print("Info: 'set_same_as_key' is a key word, will set default value as the same as key.")
                    temp_default_value = k
                else:
                    temp_default_value = default_value  # default_value保持不变
            else:
                print("default_value can only be set to dict or str types!")
                return

            if keep == 'first':
                keep_flag = k not in dict_dbms_model_type.keys()  # 只有首次满足v2k_map_rule_func才更新
            elif keep == 'last':
                keep_flag = True  # 总是更新
            else:
                print("This should never be executed! Please check your keep argument in ['first', 'last']!")
                return

            for v in values:
                v = v if not v_convert_dtype_func else v_convert_dtype_func(v)
                if v2k_map_rule_func(v, k) and keep_flag:
                    dict_dbms_model_type[k] = v

            if k not in dict_dbms_model_type.keys():  # 注意需要到最后都未被赋值的合法key才能被赋值default_value
                dict_dbms_model_type[k] = temp_default_value

    return dict_dbms_model_type


def merge_info(df_src_existing_tagging, df_src_ranking_new, df_category_labels, update_conf=None):
    series_Database_Model = df_src_ranking_new['Database Model']
    series_Multi_model_info = df_src_ranking_new['Multi_model_info']

    # 将Database Model, Multi_model_info两列中的str按','切分为类型列表，nan则返回[]，最后series纵向求和，得到拼接列表，去重后得到标签列表
    func_str_split_nan_as_emptylist = lambda x, sep: [s.strip() for s in str(x).split(sep=sep)] if pd.notna(x) else []
    substr_list_Database_Model = series_Database_Model.apply(func_str_split_nan_as_emptylist, sep=',')
    substr_list_Multi_model_info = series_Multi_model_info.apply(func_str_split_nan_as_emptylist, sep=',')
    types_Database_Model = list(set(substr_list_Database_Model.sum()))
    types_Multi_model_info = list(set(substr_list_Multi_model_info.sum()))
    types_Database_Model.sort()
    types_Multi_model_info.sort()
    # print(types_Database_Model, types_Multi_model_info)

    # 建立key:Database Model到value:Multi_model_info的映射表，
    ignore_keys = ['Multi-model']
    dict_existing_category_labels = df_category_labels.set_index(['category_label'])['category_name'].to_dict()
    dict_dbms_model_type = auto_gen_dbms_model_type_dict_by_keys(types_Database_Model, types_Multi_model_info,
                                                                 ignore_keys=ignore_keys, default_value=dict_existing_category_labels)
    # 保存dict_dbms_model_type为dataframe，并作为最终结果返回
    df_category_labels_updated = pd.DataFrame(list(dict_dbms_model_type.items()), columns=['category_label', 'category_name'])

    # 当Database Model的值仅有'Multi-model'时，替换为Multi_model_info中的首个类型的key。.replace('Multi-model', '')
    strip_blanks_inside_strs = lambda x: ','.join([s.strip() for s in str(x).split(',')]) if pd.notna(x) else ''
    drop_ignore_keys = lambda x: ','.join([s for s in strip_blanks_inside_strs(x).split(',') if s not in ignore_keys])
    main_labels_list = list(df_src_ranking_new['Database Model'].apply(drop_ignore_keys))
    main_label_names_list = []
    for main_labels in main_labels_list:
        temp_main_labels = main_labels.split(',')
        temp_main_label_names = []
        for s in temp_main_labels:
            if len(s):
                temp_main_label_names.append(dict_dbms_model_type[s])
        if len(temp_main_label_names):
            main_label_names_list.append(','.join(temp_main_label_names))
        else:
            main_label_names_list.append('')

    Multi_model_infos = list(df_src_ranking_new['Multi_model_info'].apply(strip_blanks_inside_strs))

    # 补全'Multi_model_info'
    for i in range(len(main_label_names_list)):
        main_label_names = main_label_names_list[i]
        affiliated_label_names = Multi_model_infos[i]
        if not len(affiliated_label_names):
            Multi_model_infos[i] = main_label_names
        else:
            if len(main_label_names):
                pre_list = main_label_names.split(',')
                suf_list = affiliated_label_names.split(',')
                merge_list = [pre for pre in pre_list if len(pre)]
                for suf in suf_list:
                    if suf not in pre_list and len(suf):
                        merge_list.append(suf)
                Multi_model_infos[i] = ','.join(merge_list)
            else:
                pass  # 保持不变
    # print(Multi_model_infos)
    # print(list(df_src_ranking_new['Multi_model_info']))
    df_src_ranking_new['Multi_model_info'] = Multi_model_infos

    # 反过来补全'Database Model'
    dict_dbms_model_type_reverse = {v: k for k, v in dict_dbms_model_type.items()}
    for i in range(len(main_label_names_list)):
        main_label_names = main_label_names_list[i]
        affiliated_label_names = Multi_model_infos[i]
        if not len(affiliated_label_names):
            main_label_names_list[i] = ''
        else:
            merge_list = affiliated_label_names.split(',')[0]
            main_label_names_list[i] = dict_dbms_model_type_reverse[merge_list]
    # print(main_label_names_list)
    # print(list(df_src_ranking_new['Database Model']))
    df_src_ranking_new['Database Model'] = main_label_names_list

    # 更新
    if not update_conf:
        update_conf = {
            'category': 'update__change_colname(category_label)__use_new(Database Model)',  # update values and change the column name
            'Multi_model_info': 'build__basedon(Multi_model_info)',
            'system': 'update__change_colname(DBMS)__use_new',  # update values and change the column name
            'is_open_source': 'update__reuse_old_if_cooccurrence_on(DBMS)',
            'has_company': 'update__reuse_old_if_cooccurrence_on(DBMS)',
            'github_repo_link': 'update__reuse_old_if_cooccurrence_on(DBMS)',
            'score': 'update__change_colname(Score_Nov-2022)__use_new',  # update values and change the column name
            'overall_rank': 'update__change_colname(Rank_Nov-2022)__use_new__dtype(int)',  # update values and change the column name
            'org_name': 'update__reuse_old_if_cooccurrence_on(DBMS)',  # 依赖于手动更新的列github_repo_link
            'repo_name': 'update__reuse_old_if_cooccurrence_on(DBMS)',  # 依赖于手动更新的列github_repo_link
        }

    dbtype_update_conf = {dbtype: 'recalc__calc_basedon(Multi_model_info)' for dbtype in dict_dbms_model_type.keys()}
    for k, v in dbtype_update_conf.items():  # 加入每个dict_dbms_model_type及'recalc__calc_basedon(Multi_model_info)'设置
        if k not in update_conf:
            update_conf[k] = v

    default_conf_level = {
        0: "build",
        1: "update",
        2: "recalc",
    }
    default_conf_level_sorted_pairs = sorted(default_conf_level.items(), key=lambda d: d[0], reverse=False)

    # init df_tar_automerged_colnames
    df_tar_automerged_colnames = []
    for k_colname, v_updateconf in update_conf.items():
        safe_updatable = False
        if v_updateconf:
            if k_colname in df_src_existing_tagging.columns or k_colname in df_src_ranking_new.columns or v_updateconf.startswith('recalc'):
                safe_updatable = True

        if not safe_updatable:
            print("Warning: missing the configuration of {k_colname}, try to insert a new column!".format(k_colname=k_colname))
        df_tar_automerged_colnames.append(k_colname)

    shape = (len(df_src_ranking_new), len(df_tar_automerged_colnames))
    df_tar_automerged = pd.DataFrame(np.full(shape, fill_value=np.nan), columns=df_tar_automerged_colnames)  # init all value as np.nan
    changed_colnames = []
    # print('df_tar_automerged.columns:\n', df_tar_automerged.columns)
    for _, conf_func_word in default_conf_level_sorted_pairs:
        for k_colname, v_updateconf in update_conf.items():
            v_updateconf_settings = v_updateconf.split('__')
            if conf_func_word in v_updateconf_settings:
                df_tar_automerged, changed_colnames = exe_conf(df_tar_automerged, conf_func_word, k_colname, v_updateconf_settings[1:],
                                                               df_src_existing_tagging, df_src_ranking_new, changed_colnames, dict_dbms_model_type)
    # pd.set_option('display.max_columns', None)  # 展示所有列
    # print(df_tar_automerged.head())
    return df_tar_automerged, df_category_labels_updated


def exe_conf(df_merged, conf_func_word, k_colname, v_exe_settings, df_old, df_new, changed_colnames, dict_dbms_model_type):
    if 'build' == conf_func_word:
        print(conf_func_word, ':', k_colname, v_exe_settings)
        basedon_colname = None
        for update_conf in v_exe_settings:
            if update_conf.startswith('basedon'):
                basedon_colname = re.findall(r'basedon\(([\w\s\.-]+)\)', update_conf)[0]
                print('\tbuild "{build_colname}" based on "{basedon_colname}"'.format(
                    build_colname=k_colname, update_conf=update_conf, basedon_colname=basedon_colname))

        if basedon_colname:
            df_merged[k_colname] = df_new[basedon_colname]

    elif 'update' == conf_func_word:
        temp_colname = k_colname
        primary_key = None
        use_new_flag = False
        use_new_colname = None
        reuse_old_if_cooccurrence_on = None
        dtype_setting = None
        print(conf_func_word, ':', k_colname, v_exe_settings)
        for update_conf in v_exe_settings:
            if update_conf.startswith('change_colname'):
                temp_colname = re.findall(r'change_colname\(([\w\s\.-]+)\)', update_conf)[0]
                print('\tchange_colname from "{old_colname}" to "{new_colname}"'.format(old_colname=k_colname,
                                                                                    new_colname=temp_colname))
            elif update_conf.startswith('use_new'):
                print('\t{update_conf}'.format(update_conf=update_conf))
                use_new_flag = True
                try:
                    use_new_colname = re.findall(r'use_new\(([\w\s\.-]+)\)', update_conf)[0]
                except IndexError:
                    use_new_colname = None
            elif 'primary_key' == update_conf:
                primary_key = temp_colname
                print('\t{update_conf}'.format(update_conf=update_conf))
            elif update_conf.startswith('reuse_old_if_cooccurrence'):
                reuse_old_if_cooccurrence_on = re.findall(r'reuse_old_if_cooccurrence_on\(([\w\s\.-]+)\)', update_conf)[0]
                print('\t{update_conf}'.format(update_conf=update_conf))
            elif update_conf.startswith('dtype'):
                dtype_setting = re.findall(r'dtype\(([\w\s\.-]+)\)', update_conf)[0]
                print('\t{update_conf}'.format(update_conf=update_conf))

        if temp_colname != k_colname:  # for 'update__change_colname'
            columns = list(df_merged.columns)
            idx_k_colname = columns.index(k_colname)
            columns[idx_k_colname] = temp_colname
            df_merged.columns = columns
            changed_colnames.append([k_colname, temp_colname])
        if use_new_flag:
            use_new_colname = use_new_colname or temp_colname
            # print(temp_colname, use_new_colname)
            df_merged[temp_colname] = df_new[use_new_colname]
        if reuse_old_if_cooccurrence_on:
            if not reuse_old_if_cooccurrence_on in df_merged.columns:
                print('Please use the newest colname and make sure the column has already been set '
                      'while setting the reuse_old_if_cooccurrence_on!')
                return

            newname_reuse_old_if_cooccurrence_on = reuse_old_if_cooccurrence_on
            idx_cooc_on_colname_changed = list(changed_colnames[:][1]).index(reuse_old_if_cooccurrence_on)
            if idx_cooc_on_colname_changed:
                oldname_reuse_old_if_cooccurrence_on = changed_colnames[idx_cooc_on_colname_changed][0]
            else:
                oldname_reuse_old_if_cooccurrence_on = reuse_old_if_cooccurrence_on
            # print(newname_reuse_old_if_cooccurrence_on, oldname_reuse_old_if_cooccurrence_on)
            str_strip_ignore_nan = lambda x: str(x).strip() if pd.notna(x) else np.nan
            df_merged[newname_reuse_old_if_cooccurrence_on] = df_merged[newname_reuse_old_if_cooccurrence_on].apply(str_strip_ignore_nan)
            dict_merged_oncol_reusecol = df_merged.set_index(newname_reuse_old_if_cooccurrence_on)[temp_colname].to_dict()
            df_old[oldname_reuse_old_if_cooccurrence_on] = df_old[oldname_reuse_old_if_cooccurrence_on].apply(str_strip_ignore_nan)
            dict_old_oncol_reusecol = df_old.set_index(oldname_reuse_old_if_cooccurrence_on)[k_colname].to_dict()
            # print(dict(sorted(dict_old_oncol_reusecol.items())))
            for k, v in dict_merged_oncol_reusecol.items():
                if k in list(dict_old_oncol_reusecol.keys()):
                    dict_merged_oncol_reusecol[k] = dict_old_oncol_reusecol[k] if pd.isna(v) else v
            # print(dict(sorted(dict_merged_oncol_reusecol.items())))
            df_merged_oncol_reusecol = pd.DataFrame(list(dict_merged_oncol_reusecol.items()), columns=[newname_reuse_old_if_cooccurrence_on, temp_colname])
            df_merged.update(df_merged_oncol_reusecol)
            # print(df_merged[[newname_reuse_old_if_cooccurrence_on, temp_colname]])
        if dtype_setting:
            df_merged[temp_colname] = df_merged[temp_colname].astype(dtype_setting)
        if primary_key:
            df_merged.set_index(primary_key, inplace=True)
    elif 'recalc' == conf_func_word:
        print(conf_func_word, ':', k_colname, v_exe_settings)
        recalc_colname = k_colname
        calc_basedon_colname = None
        for update_conf in v_exe_settings:
            if update_conf.startswith('calc_basedon'):
                calc_basedon_colname = re.findall(r'calc_basedon\(([\w\s\.-]+)\)', update_conf)[0]
                print('\trecalc "{recalc_colname}" based on "{calc_basedon_colname}"'.format(
                    recalc_colname=recalc_colname, update_conf=update_conf, calc_basedon_colname=calc_basedon_colname))
        if calc_basedon_colname:
            strs_comma_joined_include_s = lambda strs, s: int(s in [it.strip() for it in strs.split(',')] if pd.notna(strs) else False)
            df_merged[k_colname] = df_merged[calc_basedon_colname].apply(strs_comma_joined_include_s, s=dict_dbms_model_type[k_colname])
    else:
        print("Warning: Unavailable configuration of {k_colname}, it will be filled in default value!".format(k_colname=k_colname))
    return df_merged, changed_colnames


def merge_info_to_csv(df_src_existing_tagging, df_src_ranking_new, df_category_labels, update_conf,
                      save_automerged_path, save_category_labels_path=None):
    df_tar_automerged, df_category_labels_updated = merge_info(df_src_existing_tagging, df_src_ranking_new,
                                                               df_category_labels, update_conf)
    df_tar_automerged.to_csv(save_automerged_path, encoding=encoding, index=True)
    df_category_labels_updated.to_csv(save_category_labels_path, encoding=encoding, index=False)
    return None


if __name__ == '__main__':
    src_existing_tagging_info_path = os.path.join(pkg_rootdir, 'data/existing_tagging_info/DB_EngRank_tophalf_githubprj_summary.CSV')
    src_ranking_crawling_raw_path = os.path.join(pkg_rootdir, 'data/db_engines_ranking_table_full/ranking_crawling_202211_raw.csv')
    tar_automerged_path = os.path.join(pkg_rootdir, 'data/db_engines_ranking_table_full/ranking_crawling_202211_automerged.csv')
    src_category_labels_path = os.path.join(pkg_rootdir, 'data/existing_tagging_info/category_labels.csv')
    tar_category_labels_updated_path = os.path.join(pkg_rootdir, 'data/db_engines_ranking_table_full/category_labels_updated.csv')

    OVERWRITE_CATEGORY_LABELS = True

    df_src_existing_tagging_info = pd.read_csv(src_existing_tagging_info_path, encoding=encoding, index_col=0)
    df_src_ranking_crawling_raw = pd.read_csv(src_ranking_crawling_raw_path, encoding=encoding, index_col=False)

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

        # pd.set_option('display.max_columns', None)  # 展示所有列
        # print(df_src_existing_tagging_info.head())
        # print(df_src_ranking_crawling_raw.head())
        # print(df_category_labels)

    # 更新设置
    update_conf = {
        'category': 'update__change_colname(category_label)__use_new(Database Model)',  # update values and change the column name
        'Multi_model_info': 'build__basedon(Multi_model_info)',
        'system': 'update__change_colname(DBMS)__use_new',  # update values and change the column name
        'DBMS_insitelink': 'update__use_new',  # insert values
        'is_open_source': 'update__reuse_old_if_cooccurrence_on(DBMS)',
        'has_company': 'update__reuse_old_if_cooccurrence_on(DBMS)',
        'github_repo_link': 'update__reuse_old_if_cooccurrence_on(DBMS)',
        'score': 'update__change_colname(Score_Nov-2022)__use_new',  # update values and change the column name
        'overall_rank': 'update__change_colname(Rank_Nov-2022)__use_new__dtype(int)',  # update values and change the column name
        'org_name': 'update__reuse_old_if_cooccurrence_on(DBMS)',  # 依赖于手动更新的列github_repo_link
        'repo_name': 'update__reuse_old_if_cooccurrence_on(DBMS)',  # 依赖于手动更新的列github_repo_link
    }

    merge_info_to_csv(df_src_existing_tagging_info, df_src_ranking_crawling_raw, df_category_labels, update_conf,
                      save_automerged_path=tar_automerged_path, save_category_labels_path=tar_category_labels_updated_path)

    if OVERWRITE_CATEGORY_LABELS:
        shutil.copy(tar_category_labels_updated_path, src_category_labels_path)
