#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Python 3.7

# @Time   : 2022/11/20 22:30
# @Author : 'Lou Zehua'
# @File   : __init__.py.py 

import os
import sys

cur_dir = os.getcwd()
pkg_rootdir = os.path.dirname(os.path.dirname(cur_dir))  # os.path.dirname()向上一级，注意要对应工程root路径
if pkg_rootdir not in sys.path:
    sys.path.append(pkg_rootdir)
print('Add root directory "{}" to system path.'.format(pkg_rootdir))
