"""
Модуль с основными функциями для работы с данными в стиле
jupyter notebook с использованием spark.

Функции по максимуму не имеют состояния, для обмена информацией
используем переменные Airflow
"""

import requests, json, time, textwrap

from airflow import AirflowException

headers = {'Content-Type': 'application/json'}
host = 'http://z14-1379-dl:8998'
sessCmd = "/sessions"
stmtCmd = "/statements"

def getSessId(appName, doRaise=True):
    """ возвращает ID сессии по имени приложения в формате /ID или райзит эксепшн (можно отключить) """
    
    res = requests.get(host+sessCmd, headers=headers)
    if res:
        for sess in res.json()['sessions']:
            if sess['name']==appName:
                return "/" + str(sess['id'])
            
    # если пришли сюда - нет такого приложения...
    if doRaise:
        print("**getSessId ERROR**: no such app",appName)
        raise AirflowException
    else:
        return None
    
def createSess(name, executorMemory=None, numExecutors=None, executorCores=None):
    """ создает сессию с заданными параметрами или райзит ексепшн """
    
    args = locals()
    
    data = {
        'kind': 'pyspark'
    }
    for k,v in args.items():
        if v is not None:
            data[k] = v
    
    res = requests.post(host+sessCmd, data=json.dumps(data), headers=headers)
    if res:
        sessDict = res.json()
        sessId = "/"+str(sessDict['id'])
        while sessDict["state"]!="idle": # ждем до этого состояния...
            time.sleep(1)
            sessDict = requests.get(host+sessCmd+sessId, headers=headers).json()
    else:
        print("**createSess ERROR**: status_code",res.status_code, ", text:",res.text)
        raise AirflowException

def execStmt(appName,stmt,doFail=False):
    """ выполняет python код в сессии, дожидаясь его завершения
    райзит ексепшн, если код дал ошибку и в параметрах не задано игнорирование ошибки
    """

    ssid = getSessId(appName)
    
    r = requests.post(host+sessCmd+ssid+stmtCmd, data=json.dumps({'code': stmt }), headers=headers)
    if r.ok:
        stmtDict = r.json()
        stmtId = "/"+str(stmtDict['id'])
        while stmtDict["state"]!="available": # ждем до этого статуса
            time.sleep(1)
            r = requests.get(host+sessCmd+ssid+stmtCmd+stmtId, headers=headers)
            if r.ok:
                stmtDict = r.json()
            else:
                print("**execStmt request failed**: r ",r)
                raise AirflowException
        # проверяем результат выполнения - райзим ошибку если не ОК
        print("**execStmt finished**: output ",str(stmtDict["output"]))
        if stmtDict["output"]["status"]!="ok" and doFail:
            raise AirflowException
    else:
        print("**execStmt creation ERROR**: status_code",r.status_code, ", text:",r.text)
        if doFail:
            raise AirflowException

def closeSess(appName):
    """ закрывает сессию """

    ssid = getSessId(appName,False)
    if ssid is not None:
        requests.delete(host+sessCmd+ssid, headers=headers) # хотим что-то проверить?

def createSparkSession(appName):

    stmt = textwrap.dedent("""\
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import lit
        sp = SparkSession.builder.enableHiveSupport().getOrCreate()        
    """)

    execStmt(appName,stmt)

def loadTable(appName,tabName, pcol=None, np=None, lb=None, ub=None):
    """ если будем пользоваться, то нужно убрать имена в константы или параметры """

    stmt = textwrap.dedent("""\
        rdr = sp.read \
            .format("jdbc") \
            .option("url", "jdbc:oracle:thin:@p260unc4:1566:uncunc") \
            .option("dbtable", "{}") \
            .option("user", "staff") \
            .option("password", "staff") \
            .option("driver", "oracle.jdbc.driver.OracleDriver") \
            .option("fetchsize", 5000)
    """.format(tabName))

    if pcol is not None: # большая таблица - добавляем опции для параллелизма
        stmt += textwrap.dedent("""\
            ldf.option("lowerBound", {}) \
                .option("upperBound", {}) \
                .option("partitionColumn", "{}") \
                .option("numPartitions", {})""".format(lb,ub,pcol,np))
    stmt += "ldf = rdr.load()"

    execStmt(appName,stmt)

def storeTable(appName,tabName):
    """ если будем пользоваться, то нужно убрать параметры сохранения в константы или параметры """

    stmt = """ldf.write.option("compression","gzip").format("parquet").mode("overwrite").saveAsTable("{}")""".format(tabName)
    execStmt(appName,stmt)
