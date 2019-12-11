# -*- coding: utf-8 -*-
# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:light
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.4'
#       jupytext_version: 1.2.4
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# # Админ действия по livy

import json, requests, os

# **Настройки**

headers = {'Content-Type': 'application/json'}
host = 'http://z14-1379-dl:8998'
data = {
    'name': 'livy_test',
    'kind': 'pyspark'
}
sessCmd = "/sessions"
stmtCmd = "/statements"
os.environ["http_proxy"] = ""

# **Базовая информация о сессиях**

res = requests.get(host+sessCmd, headers=headers)
if res:
    for sess in res.json()['sessions']:
        print(sess['id'],': name',sess['name'],', state',sess['state'])
    if len(res.json()['sessions'])==0:
        print("No sessions...")
else:
    print("ERROR:")
    print(res.text)

# **Выполняющийся оператор**
#
# Не забыть поменять ID сессии

res = requests.get(host+sessCmd+"/54"+stmtCmd)
if res:
    for stmt in res.json()['statements']:
        print(stmt['id'],'>> code:\n',stmt['code'])
        print('state:',stmt['state'])
        if stmt['state'] in ['available']:
            print('OUTPUT:',stmt['output'])
        print('----------------')
    if len(res.json()['statements'])==0:
        print("No statements...")
else:
    print("ERROR:")
    print(res.text)

# **Убиение сессии**
#
# Иногда так бывает, что сессия спарка кончается, а сессия livy остается. Если ее не убить, то следующая сессия с тем же именем не запустится...
#
# Не забыть поменять ID сессии

requests.delete(host+sessCmd+"/50", headers=headers)
