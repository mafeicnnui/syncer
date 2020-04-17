export PYTHON3_HOME=/home/hopson/apps/usr/webserver/python3.6.0
export LD_LIBRARY_PATH=${PYTHON3_HOME}/lib
nohup /home/hopson/apps/usr/webserver/python3.6.0/bin/python3 /home/hopson/apps/usr/webserver/schedule_task/mysql_schedule_task.py -conf 
/home/hopson/apps/usr/webserver/schedule_task/mysql_schedule_task.ini &>/tmp/schedule_task.tmp &