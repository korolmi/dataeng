# Кратко

Записки о том, что и как настраивалось для автоматического запуска нужных сервисов при перезагрузке

## Источники информации

Кратко это описано в документации 

* systemd: https://airflow.readthedocs.io/en/stable/howto/run-with-systemd.html
* (warning) не работает с Cloudera (warning) upstart: https://airflow.readthedocs.io/en/stable/howto/run-with-upstart.html
* скрипты лежат в гитхабе (https://github.com/apache/airflow/tree/master/scripts/systemd)

## Systemd

Вот что получается:

* есть место, где лежат описания юнитов (в нашем случае - сервисов: airflow-scheduler и airflow-webserver). Это - /usr/lib/systemd/system (если такой папки нет - создаем)
* есть место, где лежит конфигурация (один файл), в нашем случае пусть это будет /etc/airflow. Тут все должно быть ровно - этот пусть прописан в сервисах
* есть место, где лежит информация о том, с какими полномочиями должен создаваться run файл, в нашем случае пусть это будет /usr/lib/tmpfiles.d (такой путь уже есть)
* нужно определить пользователя, от имени которого будут работать сервисы Airflow (в нашем примере - mluser)

Что делаем

* правим описания сервисов и копируем в /usr/lib/systemd/system (конкретный работающий на Ubuntu 16.04 пример приведен ниже)
* правим конфигурацию и копируем в /etc/airflow (пример - ниже)
* правим файл с полномочиями и копируем в /usr/lib/tmpfiles.d
* говорим sudo systemctl enable airflow-scheduler (и то же для вебсервера)
* создать директорию /run/airflow, сделать хозяином mluser

Для перезапуска выполняем

    sudo systemctl restart <service> # например airflow-scheduler

## Upstart

Пробовал настраивать для upstart - у меня поломало Cloudera (не стартовал datanode, вообще стало все плохо...) Нигде не нашел, что upstart противоречит clouder-e. Может быть, это было потому, что upstart-a не было на момент установки cloudera.

Поэтому эту веточку здесь не развиваю.

## Примеры файлов

В примерах предполагается, что Airflow использует mySQL.

### /usr/lib/systemd/system/airflow-scheduler.service

    [Unit]
    Description=Airflow scheduler daemon
    After=network.target mysql.service 
    Wants=mysql.service
    
    [Service]
    EnvironmentFile=/etc/airflow/airflow
    User=mluser
    Group=mluser
    Type=simple
    ExecStart=/usr/local/bin/airflow scheduler
    Restart=always
    RestartSec=5s
    
    [Install]
    WantedBy=multi-user.target

### /usr/lib/systemd/system/airflow-webserver.service

    [Unit]
    Description=Airflow webserver daemon
    After=network.target mysql.service
    Wants=mysql.service
    
    [Service]
    EnvironmentFile=/etc/airflow/airflow
    User=mluser
    Group=mluser
    Type=simple
    ExecStart=/usr/local/bin/airflow webserver --pid /run/airflow/webserver.pid
    Restart=on-failure
    RestartSec=5s
    PrivateTmp=true
    
    [Install]
    WantedBy=multi-user.target

### /etc/airflow/airflow

    # This file is the environment file for Airflow. Put this file in /etc/sysconfig/airflow per default
    # configuration of the systemd unit files.
    #
    AIRFLOW_CONFIG=/home/mluser/airflow/airflow.cfg
    AIRFLOW_HOME=/home/mluser/airflow

### /usr/lib/tmpfiles.d/airflow.conf

    d /run/airflow 0755 mluser mluser

