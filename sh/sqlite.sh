
 1067  tar xvfz sqlite-autoconf-3460100.tar.gz
 1068*
 1069  ./configure
 1070  make
 1071  sudo make install
 1072  /usr/local/bin/sqlite3 --version
 1073  sudo ln -sf /usr/local/bin/sqlite3 /usr/bin/sqlite3
 1074  cd
 1075  airflow db init

 [root@CMNDG-INGESTNODE03:~]# airflow db init
 Traceback (most recent call last):
   File "/usr/local/bin/airflow", line 5, in <module>
     from airflow.__main__ import main
   File "/usr/local/lib/python3.6/site-packages/airflow/__init__.py", line 35, in <module>
     from airflow import settings
   File "/usr/local/lib/python3.6/site-packages/airflow/settings.py", line 35, in <module>
     from airflow.configuration import AIRFLOW_HOME, WEBSERVER_CONFIG, conf  # NOQA F401
   File "/usr/local/lib/python3.6/site-packages/airflow/configuration.py", line 1187, in <module>
     conf.validate()
   File "/usr/local/lib/python3.6/site-packages/airflow/configuration.py", line 224, in validate
     self._validate_config_dependencies()
   File "/usr/local/lib/python3.6/site-packages/airflow/configuration.py", line 278, in _validate_config_dependencies
     f"error: sqlite C library version too old (< {min_sqlite_version}). "
 airflow.exceptions.AirflowConfigException: error: sqlite C library version too old (< 3.15.0). See https://airflow.apache.org/docs/apache-airflow/2.2.5/howto/s                                                                                          et-up-database.html#setting-up-a-sqlite-database

 1077  sqlite3 --version

 1081  python -c "import sqlite3; print(sqlite3.sqlite_version)"

 [root@CMNDG-INGESTNODE03:~]#  python -c "import sqlite3; print(sqlite3.sqlite_version)"
3.7.17
[root@CMNDG-INGESTNODE03:~]# echo "/usr/local/lib" | sudo tee /etc/ld.so.conf.d/sqlite.conf
/usr/local/lib
[root@CMNDG-INGESTNODE03:~]# sudo ldconfig
[root@CMNDG-INGESTNODE03:~]#  python -c "import sqlite3; print(sqlite3.sqlite_version)"
3.46.1
[root@CMNDG-INGESTNODE03:~]# airflow db init

 1082  sudo alternatives --config python

 remettre python à 3.x

 
 1083  airflow db init
 1084   python -c "import sqlite3; print(sqlite3.sqlite_version)"
 1085  echo "/usr/local/lib" | sudo tee /etc/ld.so.conf.d/sqlite.conf
 1086  sudo ldconfig
 1087   python -c "import sqlite3; print(sqlite3.sqlite_version)"
 1088  airflow db init
 1089  history
