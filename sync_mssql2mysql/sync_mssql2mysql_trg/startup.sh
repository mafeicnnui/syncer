export APP_PATH=/home/hopson/apps/usr/webserver
export PYTHON3_HOME=/home/hopson/apps/usr/webserver/python3.6.0
export SYNC_HOME=/home/hopson/apps/usr/webserver/dba/sync_park_flow
export LD_LIBRARY_PATH=${PYTHON3_HOME}/lib
export PATH=.:${PYTHON3_HOME}/bin:$PATH
nohup  python3 ${SYNC_HOME}/sync_sqlserver2mysql.py -conf ${SYNC_HOME}/sync_sqlserver2mysql.ini  -debug &> ${SYNC_HOME}/sync_sqlserver2my
sql.log &