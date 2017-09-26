# encoding: utf-8

# TODO MySQLdb: is it indispensable?
import MySQLdb


def create_connection(host, port, db, user, password):
    dbconfig_local = dict()
    dbconfig_local['host'] = host
    dbconfig_local['port'] = port
    dbconfig_local['user'] = user
    dbconfig_local['db'] = db
    dbconfig_local['passwd'] = password
    dbconfig_local['charset'] = 'utf8'
    conn = MySQLdb.connect(host=dbconfig_local['host'], user=dbconfig_local['user'],
                           passwd=dbconfig_local['passwd'], db=dbconfig_local['db'],
                           port=dbconfig_local['port'], charset=dbconfig_local['charset'])
    return conn


def get_jzts_connection():
    return create_connection('10.2.0.24', 3306, 'jzts', 'jzts', 'jzam123')


class DbManager(object):
    """
    docstring
    """
    
    def __init__(self):
        self.conns = {}
    
    def create_connection(self, host, port, db, user, password, conn_name):
        dbconfig_local = dict()
        dbconfig_local['host'] = host
        dbconfig_local['port'] = port
        dbconfig_local['user'] = user
        dbconfig_local['db'] = db
        dbconfig_local['passwd'] = password
        dbconfig_local['charset'] = 'utf8'
        conn = MySQLdb.connect(host=dbconfig_local['host'], user=dbconfig_local['user'],
                               passwd=dbconfig_local['passwd'], db=dbconfig_local['db'],
                               port=dbconfig_local['port'], charset=dbconfig_local['charset'])
        if conn_name != '':
            if conn_name in self.conns:
                print " Connection name %s has been used, failed to create connection" % (conn_name)
                conn._close()
                return None
            else:
                self.conns[conn_name] = conn
        return conn
    
    def get_connection(self, name):
        return self.conns.get(name, None)
