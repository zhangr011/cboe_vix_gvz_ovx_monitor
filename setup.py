#encoding: UTF-8

import ast
import re
from setuptools import setup, find_packages

def get_version_string():
    global version
    with open("cboe_monitor/__init__.py", "rb") as f:
        version_line = re.search(
            r"__version__\s+=\s+(.*)", f.read().decode("utf-8")
        ).group(1)
        return str(ast.literal_eval(version_line))

setup(
    name = "cboe_monitor",
    version = get_version_string(),
    author = 'zhangr011',
    include_package_data = True,
    packages = find_packages(exclude = ["test"]))
