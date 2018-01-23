#!/usr/local/bin/python2.7
import sys
import re
import time

import datetime
import traceback

import elasticsearch
from elasticsearch import Elasticsearch, helpers
import pymysql
import pprint

from multiprocessing.pool import ThreadPool

from db_conns import db_conns
import json

pp = pprint.PrettyPrinter(indent=4)

from tools import load_conf
conf = load_conf()

import logging 
logging.basicConfig(format="%(asctime)s - %(name)s - [ %(levelname)s ] - [ %(filename)s:%(funcName)s():%(lineno)s ] - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

PROCESSLIST_QUERY = 'SHOW FULL PROCESSLIST'
STATUS_QUERY = 'SHOW STATUS'
SHOW_VARIABLES_QUERY = 'SHOW variables LIKE \'%max%\''
CONNECTION_PERCENTAGE_QUERY = """
SELECT ROUND( ( pl.threads_connected / gv.max_connections ) * 100, 2) as percentage_used_connections FROM
 ( SELECT VARIABLE_VALUE as threads_connected from information_schema.global_status WHERE variable_name = 'THREADS_CONNECTED' ) AS pl, 
 ( SELECT VARIABLE_VALUE AS max_connections FROM information_schema.global_variables WHERE variable_name = 'MAX_CONNECTIONS' ) AS gv;
"""

METRICS = {
    'QPS' : 'Queries',
    'Queries_Per_Second' : 'Queries',
    'Connections' : 'Connections',
    'Connections_Per_Second' : 'Connections',
    'Qcache_inserts' : 'Qcache_inserts',
    'Qcache_hits' : 'Qcache_hits',
    'Qcache_queries_in_cache' : 'Qcache_queries_in_cache',
    'Slow_queries' : 'Slow_queries',
    'Aborted_clients' : 'Aborted_clients',
    'Aborted_connects' : 'Aborted_connects',
    'Innodb_buffer_pool_reads_Per_Second' : 'Innodb_buffer_pool_reads',
    'Com_delete_per_second' : 'Com_delete',
    'Com_commit_per_second' : 'Com_commit',
    'Com_flush_per_second' : 'Com_flush',
    'Com_insert_per_second' : 'Com_insert',
    'Com_purge_per_second' : 'Com_purge',
    'Com_select_per_second' : 'Com_select',
    'Com_set_option_per_second' : 'Com_set_option',
    'Com_update_per_second' : 'Com_update',
}

try:
    ES_CLIENT = Elasticsearch(conf['elasticsearch']['hosts'], **conf['elasticsearch']['args'])
except Exception as e:
    logger.error('Unable to connect to ES Cluster, reason: %s' % (e))
    sys.exit(1)

'''
    Gets a connection and a cursor to our database
'''
def establish_db_conn(db_connection):
    try:
        db = pymysql.connect(host=db_connection['host'], port=db_connection['port'], user=db_connection['username'], passwd=db_connection['password'], db=db_connection.get('db', 'information_schema'))
        dbc = db.cursor(pymysql.cursors.DictCursor)
        return db, dbc
    except Exception as e:
        logger.exception('Cant establish a connection to the DB: %s, reason: %s' % ( db_connection['name'], e ))
    
'''
    returns back the index name string we need for elasticsearch
'''
def getindex():
    now = datetime.datetime.utcnow().strftime('%Y.%m.%d')
    index_name = 'mysql-mon-{0}'.format(now)
    return index_name

'''
    Takes a mysql 'Variable_name' => 'Value' response,
    and returns a dict contain the { 'key': 'val' }
'''
def normalize_mysql_var_val(dictionary_resultset, key, value):
    return {row[key] : row[value] for row in dictionary_resultset}

'''
    runs a mysql status, returns a dict of of the key=>val mapped
'''
def get_mysql_status(dbc):
    try:
        dbc.execute(STATUS_QUERY)
        return normalize_mysql_var_val(dbc.fetchall(), 'Variable_name', 'Value')
    except Exception as e:
        logger.exception('Unable to run query - "show status" - reason: %s' % (e))

def get_mysql_variables(dbc):
    try:
        dbc.execute(SHOW_VARIABLES_QUERY)
        return normalize_mysql_var_val(dbc.fetchall(), 'Variable_name', 'Value')
    except Exception as e:
        logger.exception('Unable to run query - "show variables" - reason: %s' % (e))

'''
    Does some diffing of various variables to get our 'per second' metrics
'''
def normalize_mysql_conn_stats(s1, s2):
    try:
        data_dict = {}
        for friendly_name, mysql_name in METRICS.iteritems():
            data_dict[friendly_name] = int(s2[mysql_name]) - int(s1[mysql_name])

        data_dict['Threads_running'] = int(s2.get('Threads_running'))
        data_dict['Threads_connected'] = int(s2.get('Threads_connected'))
        if s1.get('Opened_files'):
            data_dict['Opened_files'] = int(s2['Opened_files']) - int(s1['Opened_files'])

        data_dict['Qcache_hit_pctg'] = float(100 * float(data_dict['Qcache_hits'])/float(data_dict['QPS']))

    except Exception as e:
        logger.exception('Unable to normalize mysql connection stats, reason: %s' % (e))

    return data_dict


def worker(dbconn):
    try:
        start_time = time.clock()
        name = dbconn['name']

        logger.info('Starting on: %s' % (name))

        db, dbc = establish_db_conn(dbconn)

        s1 = get_mysql_status(dbc)
        time.sleep(1)
        s2 = get_mysql_status(dbc)

        # mysql status
        #TODO: Can we rename _type to doc_type? I think so, but later in the loop we have it expressly as a key in data_dict
        _type = 'mysql-connection-stats'
        data_dict = {'@timestamp' : datetime.datetime.utcnow().isoformat(),
                     'host' : name,
                     'type' : _type,
                     'hostname' : name,
                     'db_host': name,
                     'connection_name': name,
                     'connection_hostname': dbconn['host'],
                     _type : normalize_mysql_conn_stats(s1, s2)}

        index_it = ES_CLIENT.index(index=getindex(), doc_type=_type, body=data_dict)
        logger.info('%s - mysql-connection-stats ran: %s' % (name, index_it))

        # Processlist
        processlist_events = []
        sleeps = 0
        dbc.execute(PROCESSLIST_QUERY)
        for r in dbc.fetchall():
            # If the query is in a Sleep state, lets skip it so we dont send unecessary data to ES
            # Just count them and put a summary in ES
            if 'Sleep' in r.get('Command'):
                sleeps += 1
                continue

            data_dict = {
                '@timestamp': datetime.datetime.utcnow().isoformat(),
                '_type' : 'processlist',
                '_index' : getindex(),
                'host' : str(name),
                'db_host': str(name),
                'hostname' : str(name),
                'connection_name': name,
                'connection_hostname': dbconn['host'],
                'plist' : dict(r)
            }


            if r['Info'] is not None:
                # short_query is the first 100 bytes of our full query, useful for aggregating a bunch of similar queries that might be an issue
                short_query = r['Info'][:100]
                query_type = r.get('Info', ' ').split()[0]
                data_dict['plist']['QueryShortName'] = short_query
                data_dict['plist']['QueryType'] = query_type

                # Do some simple parsing to determine the type of query we have; select, update, insert, etc
                try:
                    query = r.get('Info', '')
                    parts = query.split()
                    if 'select' in query_type.lower():
                        try:
                            regex = r'^.*select.*\s*from\s*(.*?) .*$'
                            match = re.search(regex, query, re.I|re.M)
                            if match:
                                table = match.group(1)
                        except:
                            pass
                    elif 'update' in query_type.lower():
                        table = parts[1]
                    elif 'insert' in query_type.lower() or 'delete' in query_type.lower():
                        table = parts[2]

                    if '.' in table:
                        table_parts = table.split('.')
                        table = table_parts[1]

                    table = table.replace('`', '')
                except:
                    table = 'not_extracted'

                data_dict['plist']['QueryAgainstTable'] = table
                data_dict['plist']['QueryTable'] = table
                data_dict['plist']['Table'] = table

            src_host, src_port = r.get('Host', '0.0.0.0:0').split(':')
            processlist_events.append(data_dict)

        index_processlist = helpers.bulk(ES_CLIENT, processlist_events)
        if index_processlist:
            logger.info('%s - Indexed %s processlist events' % (name, len(processlist_events)))

        logger.info('%s - I found %s sleep(s)' % (name, sleeps))

        # index the sleep number
        ES_CLIENT.index(index=getindex(), doc_type='processlist', body={'@timestamp': datetime.datetime.utcnow().isoformat(), 'host': name, 'hostname': name, 'sleeping_connections': sleeps})

        # Connection percentage
        dbc.execute(CONNECTION_PERCENTAGE_QUERY)
        connection_pctg = float(dbc.fetchall()[0]['percentage_used_connections'])
        logger.info('%s - Connection Percentage is %s' % (name, connection_pctg))

        index_it = ES_CLIENT.index(index=getindex(), doc_type='mysql-connection-stats', body={'@timestamp': datetime.datetime.utcnow().isoformat(), 'host': name, 'hostname': name, 'connection_percentage': connection_pctg})

        db.close()
        end_time = time.clock()
        time_taken = (end_time - start_time) / 1000
        logger.info('%s - Worker took %s to run' % (name, time_taken))
        if time_taken >= 1:
            logger.error('%s - Worker thread took a long time to run: %s ...' % (name, time_taken))
    except Exception as exc:
        #TODO: Not ideal, but it'll allow us to not accidentally kill other workers w/ an unhandled exception
        logger.exception('%s - Caught general exception in worker function: %s' % (name, exc))


def main():
    logger.info('Starting...')
    pool = ThreadPool(processes=len(db_conns))
    try:
        # Thread out for each database
        pool.map(worker, db_conns)
        pool.close()
        pool.join()
    #TODO: Shore this up, catch a more targeted exception once worker function is proofed.
    #Basically this should be a block you can only hit via threading OS level exceptions if we proof the worker func
    except Exception as e:
        logger.exception('Caught exception in main!') 
        # Clean up any zombie threads hanging around
        pool.terminate()
    logger.info('Ending...')

if __name__ == '__main__':
    while True:
        main()
        time.sleep(conf.get('interval', 10))

