import sys
from tools import load_conf

conf = load_conf()
db_conns = conf.get('db_conns')
