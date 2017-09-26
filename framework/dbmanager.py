import MySQLdb

def createConnection(host, port, db, user, password):  
        dbconfig_local = {} 
        dbconfig_local['host'   ] = host
        dbconfig_local['port'   ] = port
        dbconfig_local['user'   ] = user
        dbconfig_local['db'     ] = db
        dbconfig_local['passwd' ] = password
        dbconfig_local['charset'] = 'utf8'
        conn = MySQLdb.connect(host   = dbconfig_local['host'  ] , user    = dbconfig_local['user'   ] , \
                               passwd = dbconfig_local['passwd'] , db      = dbconfig_local['db'     ] , \
                               port   = dbconfig_local['port'  ] , charset = dbconfig_local['charset'] ) 
        return conn

def getJztsConnection():        
    return createConnection('10.2.0.24',3306, 'jzts', 'jzts', 'jzam123')


class DbManager(object):
    '''
    classdocs
    '''
    

    def __init__(self):
        self.conns = {}
        '''
        Constructor
        '''
    def createConnection(self, host, port, db, user, password, conn_name):  
        dbconfig_local = {} 
        dbconfig_local['host'   ] = host
        dbconfig_local['port'   ] = port
        dbconfig_local['user'   ] = user
        dbconfig_local['db'     ] = db
        dbconfig_local['passwd' ] = password
        dbconfig_local['charset'] = 'utf8'
        conn = MySQLdb.connect(host   = dbconfig_local['host'  ] , user    = dbconfig_local['user'   ] , \
                               passwd = dbconfig_local['passwd'] , db      = dbconfig_local['db'     ] , \
                               port   = dbconfig_local['port'  ] , charset = dbconfig_local['charset'] ) 
        if conn_name != '':
            if self.conns.has_key(conn_name):
                print " Connection name %s has been used, failed to create connection" %(conn_name)
                conn.close()
                return None
            else :
                self.conns[conn_name] = conn 
        return conn
    
    def getConnection(self,name):
        return self.conns.get(name, None)