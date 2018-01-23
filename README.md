# MySQL-Mon

Monitor your MySQL Database Performance

## Description 
A tool to extract metrics from your in-house, on-prem or cloud based MySQL Instances.  

Pull statistics around query performance, status, and connection percentage.  

This has been tested against standard MySQL installs, as well as RDS and Aurora MySQL instances.  

## Requirements:
* python 2.7, python 3.4+
* pip
* mysql 4+ - Has been tested against RDS and Aurora mysql instances as well

## Install:
1. Copy config.yml.example to config.yml and edit the sections to fill in information about your MySQL databases and Elasticsearch cluster where the metrics will be stored
2. Run the following commands from this directory:
```
virtualenv env
source env/bin/activate
pip install -r requirements.txt
```
3. Now run it:  `python mysql-mon.py`
4. If you want to this to run all the time, use something like supervisord to handle that for you --
Put this in `/etc/supervisor.d/mysql-mon.conf` - we assume that MySQL-Mon is installed in `/opt/monitor/mysql-mon`
```
[program:mysql-mon]
autostart=true
startretries=1000
process_name=%(program_name)s
stdout_logfile=/var/log/supervisor/%(program_name)s_out.log
stdout_logfile_maxbytes=50MB
stdout_logfile_backups=10
stderr_logfile=/var/log/supervisor/%(program_name)s_error.log
stderr_logfile_maxbytes=50MB
stderr_logfile_backups=10
command=/opt/monitor/mysql-mon/env/bin/python /opt/monitor/mysql-mon/mysql-mon.py
```

Happy monitoring!
