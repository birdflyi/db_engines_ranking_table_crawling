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
                                          v2k_map_rule_func=str.startswith, keep='first',
                                          default_value='set_same_as_key', drop_unused_default_values=True):
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

    if type(default_value) == dict and not drop_unused_default_values:
        for k, v in dict(default_value).items():
            if v not in dict_dbms_model_type.values() and k not in dict_dbms_model_type.keys():
                dict_dbms_model_type[k] = v

    return dict_dbms_model_type


def merge_info(df_src_existing_tagging, df_src_ranking_new, df_category_labels, update_conf=None, onehot_multi_model=True):
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
    # the category_label(as key) must be unique, while the category_name may not.
    # set drop_unused_default_values=False to keep unused default key-values in category_labels.csv
    dict_dbms_model_type = auto_gen_dbms_model_type_dict_by_keys(types_Database_Model, types_Multi_model_info,
                                                                 ignore_keys=ignore_keys, default_value=dict_existing_category_labels,
                                                                 drop_unused_default_values=False)
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
            if len(s) and dict_dbms_model_type[s] is not np.nan:
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
    for k in dict_dbms_model_type.keys():
        if k not in dict_dbms_model_type_reverse.keys():  # if the label is just equal to category string, map to itself
            dict_dbms_model_type_reverse[k] = k
    for i in range(len(main_label_names_list)):
        main_label_names = main_label_names_list[i]
        affiliated_label_names = Multi_model_infos[i]
        if not len(affiliated_label_names):
            main_label_names_list[i] = ''
        else:
            first_aff_label_name = affiliated_label_names.split(',')[0]
            if first_aff_label_name in dict_dbms_model_type_reverse.keys():
                main_label_names_list[i] = dict_dbms_model_type_reverse[first_aff_label_name]
            else:
                temp_aff_label_names = affiliated_label_names.split(',')
                for s in temp_aff_label_names:
                    if any([ignore_key in s for ignore_key in ignore_keys]):
                        continue
                    else:
                        # e.g. Handling messy formats like "CockroachDB supports relational, semi-structured JSON/document, vector/embedding, geospatial, and typed categorical/enumeration data, all exposed via SQL, on the same strongly consistent, globally distributed data platform."
                        if any([k in s or v in s for k, v in dict_dbms_model_type.items()]):
                            for k, v in dict_dbms_model_type.items():
                                if k in s or v in s:
                                    main_label_names_list[i] = k
                                    break
                                else:
                                    continue
    # print(main_label_names_list)
    # print(list(df_src_ranking_new['Database Model']))
    df_src_ranking_new['Database Model'] = main_label_names_list

    # 更新
    if not update_conf:
        update_conf = {
            'category': 'update__change_colname_as(category_label)__use_new(Database Model)',  # update values and change the column name
            'Multi_model_info': 'build__basedon(Multi_model_info)',
            'system': 'update__change_colname_as(DBMS)__use_new',  # update values and change the column name
            'is_open_source': 'update__reuse_old_if_cooccurrence_on(DBMS)',
            'has_company': 'update__reuse_old_if_cooccurrence_on(DBMS)',
            'github_repo_link': 'update__reuse_old_if_cooccurrence_on(DBMS)',
            'score': 'update__change_colname_as(Score_Nov-2022)__use_new',  # update values and change the column name
            'overall_rank': 'update__change_colname_as(Rank_Nov-2022)__use_new__dtype(int)',  # update values and change the column name
            'org_name': 'update__reuse_old_if_cooccurrence_on(DBMS)',  # 依赖于手动更新的列github_repo_link
            'repo_name': 'update__reuse_old_if_cooccurrence_on(DBMS)',  # 依赖于手动更新的列github_repo_link
        }

    if onehot_multi_model:
        dbtype_update_conf = {dbtype: 'recalc__calc_basedon(Multi_model_info)' for dbtype in dict_dbms_model_type.keys()}
    else:
        dbtype_update_conf = {}

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
    changed_colname_pairs = []
    # print('df_tar_automerged.columns:\n', df_tar_automerged.columns)
    for _, conf_func_word in default_conf_level_sorted_pairs:
        for k_colname, v_updateconf in update_conf.items():
            v_updateconf_settings = v_updateconf.split('__')
            if conf_func_word in v_updateconf_settings:
                df_tar_automerged, changed_colname_pairs = exe_conf(df_tar_automerged, conf_func_word, k_colname, v_updateconf_settings[1:],
                                                               df_src_existing_tagging, df_src_ranking_new, changed_colname_pairs, dict_dbms_model_type)
            else:
                pass
        print(f"--{conf_func_word} done!--")
    # pd.set_option('display.max_columns', None)  # 展示所有列
    # print(df_tar_automerged.head())
    return df_tar_automerged, df_category_labels_updated


def exe_conf(df_merged, conf_func_word, k_colname, v_exe_settings, df_old, df_new, changed_colname_pairs, dict_dbms_model_type):
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
        colname_dfold = k_colname
        colname_dfnew = k_colname  # 默认操纵相同的colname来合并
        primary_key = None
        change_colname_flag = False
        use_new_flag = False
        use_new_colname_flag = False
        colname_dfmerge = colname_dfold  # 懒惰模式，默认使用dfold的列名，当配置change_colname_as时使用相应的列名，未配置change_colname_as时，若配置了use_new_col，则使用dfnew的列名
        colname_cooccurrence_on = None
        dtype_setting = None
        print(conf_func_word, ':', k_colname, v_exe_settings)
        for update_conf in v_exe_settings:
            if update_conf.startswith('change_colname_as'):
                temp_colname = re.findall(r'change_colname_as\(([\w\s\.-]+)\)', update_conf)[0]
                change_colname_flag = True
                colname_dfmerge = temp_colname
                print('\tchange_colname from "{old_colname}" to "{new_colname}"'.format(old_colname=k_colname,
                                                                                    new_colname=temp_colname))
            elif update_conf.startswith('use_new'):
                if update_conf == "use_new":
                    update_conf = "use_new_col"
                    print("\tWarning: 'use_new' is obsoleted! Try to convert as new default format 'use_new_col'!")
                    print("\t\tIf you want to keep the column name of existing table, try 'use_new_only_val(COLNAME)'.")
                elif re.findall(r'use_new\(([\w\s\.-]+)\)', update_conf):
                    update_conf = update_conf.replace("use_new(", "use_new_only_val(")
                    print("\tWarning: 'use_new(COLNAME)' is obsoleted! Try to convert as new default format 'use_new_only_val(COLNAME)'!")
                    print("\t\tIf you want to use the column name and values by new table column, try 'use_new_col'.")

                print('\t{update_conf}'.format(update_conf=update_conf))
                if update_conf.startswith('use_new_only_val'):
                    # 当使用use_new_only_val且未使用change_colname_as时，colname_dfold默认作为colname_dfmerge
                    try:
                        colname_dfnew = re.findall(r'use_new_only_val\(([\w\s\.-]+)\)', update_conf)[0]
                        use_new_flag = True
                        use_new_colname_flag = False
                    except IndexError:
                        colname_dfnew = None
                        print(f'ValueError: Wrong settings in update_conf!')
                elif update_conf.startswith('use_new_col'):
                    # 当使用use_new_col且未使用change_colname_as时，colname_dfnew默认作为colname_dfmerge
                    if update_conf == "use_new_col":
                        update_conf = "use_new_col(" + k_colname + ")"
                    try:
                        colname_dfnew = re.findall(r'use_new_col\(([\w\s\.-]+)\)', update_conf)[0]
                        use_new_flag = True
                        use_new_colname_flag = True
                    except IndexError:
                        colname_dfnew = None
                        print(f'ValueError: Wrong settings in update_conf!')

            elif 'primary_key' == update_conf:
                primary_key = colname_dfmerge
                print('\t{update_conf}'.format(update_conf=update_conf))
            elif update_conf.startswith('reuse_old_if_cooccurrence'):
                colname_cooccurrence_on = re.findall(r'reuse_old_if_cooccurrence_on\(([\w\s\.-]+)\)', update_conf)[0]
                print('\t{update_conf}'.format(update_conf=update_conf))
            elif update_conf.startswith('dtype'):
                dtype_setting = re.findall(r'dtype\(([\w\s\.-]+)\)', update_conf)[0]
                if str(dtype_setting).lower().startswith('int'):
                    dtype_setting = 'Int64'
                    print("\tWarning: use default 'Int64' dtype!")
                print('\t{update_conf}'.format(update_conf=update_conf))

        if change_colname_flag:  # for 'update__change_colname_as'
            columns = list(df_merged.columns)
            idx_k_colname = columns.index(colname_dfold)
            columns[idx_k_colname] = colname_dfmerge
            df_merged.columns = columns
            changed_colname_pairs.append([colname_dfold, colname_dfmerge])
        if use_new_flag:
            if not change_colname_flag:
                colname_dfmerge = colname_dfnew if use_new_colname_flag else colname_dfold
            else:
                pass
            try:
                df_merged[colname_dfmerge] = df_new[colname_dfnew]
            except KeyError:
                print(f"KeyError: '{colname_dfnew}' should be in df_new.columns: {df_new.columns}\n")
        if colname_cooccurrence_on:  # 选取colname_cooccurrence_on时，以更新时修改colname完成后的名字为准
            if colname_cooccurrence_on not in df_merged.columns:
                print('Please use the newest colname and make sure the column has already been set '
                      'while setting the reuse_old_if_cooccurrence_on!')
                return

            new_colname_cooccurrence_on = colname_cooccurrence_on
            old_colname_cooccurrence_on = colname_cooccurrence_on  # 先默认取最新的值，再按改名表changed_colname_pairs来找出原来的列名
            if len(changed_colname_pairs):
                # 修改colname完成后的名字在changed_colname_pairs的第2个位置，即changed_colname_pairs[:][1]，
                # 若在此位置匹配到colname_cooccurrence_on，则说明用作键的colname被修改过，需要找到对应位置的旧colname索引已标注的表
                try:
                    idx_cooc_on_colname_changed = list(np.array(changed_colname_pairs)[:, 1]).index(colname_cooccurrence_on)
                except ValueError:
                    idx_cooc_on_colname_changed = None
                if idx_cooc_on_colname_changed:  # 当colname_cooccurrence_on也有改动，并且已改动完成时，需要找回改动前的列名来索引已标注的表
                    old_colname_cooccurrence_on = changed_colname_pairs[idx_cooc_on_colname_changed][0]

            str_strip_ignore_nan = lambda x: str(x).strip() if pd.notna(x) else np.nan
            df_new[new_colname_cooccurrence_on] = df_new[new_colname_cooccurrence_on].apply(str_strip_ignore_nan)
            try:
                dict_new_oncol_reusecol = df_new.set_index(new_colname_cooccurrence_on)[colname_dfnew].to_dict()
            except KeyError:
                dict_new_oncol_reusecol = dict(zip(list(df_new[new_colname_cooccurrence_on]), [np.nan]*len(df_new)))
            df_old[old_colname_cooccurrence_on] = df_old[old_colname_cooccurrence_on].apply(str_strip_ignore_nan)
            dict_old_oncol_reusecol = df_old.set_index(old_colname_cooccurrence_on)[colname_dfold].to_dict()
            # print(dict(sorted(dict_old_oncol_reusecol.items())))
            dict_merged_oncol_reusecol = dict_new_oncol_reusecol
            for k, v in dict_merged_oncol_reusecol.items():
                if k in list(dict_old_oncol_reusecol.keys()):
                    dict_merged_oncol_reusecol[k] = dict_old_oncol_reusecol[k] if pd.isna(v) else v
            # print(dict(sorted(dict_merged_oncol_reusecol.items())))
            df_merged_oncol_reusecol = pd.DataFrame(list(dict_merged_oncol_reusecol.items()), columns=[new_colname_cooccurrence_on, colname_dfmerge])
            df_merged.update(df_merged_oncol_reusecol)
        if dtype_setting:
            df_merged = df_merged.astype({colname_dfmerge: dtype_setting})
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
    return df_merged, changed_colname_pairs


def merge_info_to_csv(df_src_existing_tagging, df_src_ranking_new, df_category_labels, update_conf,
                      save_automerged_path, save_category_labels_path=None):
    df_tar_automerged, df_category_labels_updated = merge_info(df_src_existing_tagging, df_src_ranking_new,
                                                               df_category_labels, update_conf)
    df_tar_automerged.to_csv(save_automerged_path, encoding=encoding, index=False)
    df_category_labels_updated.to_csv(save_category_labels_path, encoding=encoding, index=False)
    return None


def trim_spaces(s):
    return re.sub(r'\s+', " ", s)


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
        'category': 'update__change_colname_as(category_label)__use_new(Database Model)',
        # update values and change the column name
        'Multi_model_info': 'build__basedon(Multi_model_info)',
        'system': 'update__change_colname_as(DBMS)__use_new(DBMS)',  # update values and change the column name
        'DBMS_insitelink': 'update__use_new',  # insert values
        'is_open_source': 'update__change_colname_as(has_github_repo)__reuse_old_if_cooccurrence_on(DBMS)',
        'has_company': 'update__reuse_old_if_cooccurrence_on(DBMS)',
        'github_repo_link': 'update__reuse_old_if_cooccurrence_on(DBMS)',
        'score': 'update__change_colname_as(Score_Nov-2022)__use_new(Score_Nov-2022)',
        # update values and change the column name
        'overall_rank': 'update__change_colname_as(Rank_Nov-2022)__use_new(Rank_Nov-2022)__dtype(int)',
        # update values and change the column name
        'org_name': 'update__reuse_old_if_cooccurrence_on(DBMS)',  # 依赖于手动更新的列github_repo_link
        'repo_name': 'update__reuse_old_if_cooccurrence_on(DBMS)',  # 依赖于手动更新的列github_repo_link
    }

    df_src_existing_tagging_info["system"] = df_src_existing_tagging_info["system"].apply(trim_spaces)
    df_src_ranking_crawling_raw["DBMS"] = df_src_ranking_crawling_raw["DBMS"].apply(trim_spaces)
    merge_info_to_csv(df_src_existing_tagging_info, df_src_ranking_crawling_raw, df_category_labels, update_conf,
                      save_automerged_path=tar_automerged_path, save_category_labels_path=tar_category_labels_updated_path)

    if OVERWRITE_CATEGORY_LABELS:
        shutil.copy(tar_category_labels_updated_path, src_category_labels_path)
