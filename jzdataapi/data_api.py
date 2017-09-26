import time

import jrpc_py
# import jrpc
import utils


#def set_log_dir(log_dir):
#    if log_dir:
#        jrpc.set_log_dir(log_dir)

#class DataApiCallback:
#    """DataApi Callback
#
#    def on_quote(quote):
#        pass
#        
#    def on_connection()
#    """
#
#    def __init__(self):
#        self.on_quote = None

class DataApi:
    
    def __init__(self, addr, use_jrpc=False):
        """Create DataApi client.
        
        If use_jrpc, try to load the C version of JsonRpc. If failed, use pure
        Python version of JsonRpc.
        """
        self._remote = None
#        if use_jrpc:
#            try:
#                import jrpc
#                self._remote = jrpc.JRpcClient()
#            except Exception as e:
#                print "Can't load jrpc", e.message
        
        if not self._remote:
            self._remote = jrpc_py.JRpcClient()

        self._remote.on_rpc_callback = self._on_rpc_callback
        self._remote.on_disconnected = self._on_disconnected
        self._remote.on_connected    = self._on_connected
        self._remote.connect(addr)

        self._on_jsq_callback = None

        self._connected   = False
#        self._username    = ""
#        self._password    = ""
        self._data_format = "default"
        self._callback = None
        self._schema = []
        self._schema_id = 0

    def __del__(self):
        self._remote.close()
    

    def _on_disconnected(self):
        """JsonRpc callback"""
#        print "DataApi: _on_disconnected"
        self._connected = False
        
        if self._callback:
            self._callback("connection", False)

    def _on_connected(self):
        """JsonRpc callback"""
        #print "DataApi: _on_connected"
        self._connected = True

        if self._callback:
            self._callback("connection", True)

    def _check_session(self):
        if not self._connected:
            return (False, "no connection")
        else:
            return (True, "")
#        r, msg = self._do_login()
#        if not r: return (r, msg)
#        if self._strategy_id :
#            return self._do_use_strategy()
#        else :
#            return (r,msg)


    def connect(self , timeout=3):
        """Connect to server
        """
        for i in xrange(timeout):
            if self._connected:
                break
            time.sleep(1)
        if self._connected:
            return (True, "0,")
        else:
            return (False, "-1,timeout")

    def close(self):
        self._remote.close()


    def set_data_format(self, format):
        """Set queried data format.
        
        Available formats are:
            ""        -- Don't convert data, usually the type is map
            "obj"     -- Convert map to object
            "pandas"  -- Convert table likely data to DataFrame
        """
        self._data_format = format
        
    def _get_format(self, format, default_format):
        if format:
            return format
        elif self._data_format != "default":
            return self._data_format
        else:
            return default_format

    def set_callback(self, callback):
        self._callback = callback

    def _convert_quote_ind(self, quote_ind):
        """Convert original quote_ind to an object or a map.
        
        The original quote_ind contains field index instead of field name!
        """
        
        if quote_ind != self._schema_id:
            return None

        indicators = quote_ind.indicators
        values     = quote_ind.values

        max_index = len(self._schema)

        quote = {}
        for i in xrange(len(indicators)):
            if indicators[i] < max_index: 
                quote[self._schema[indicators[i]].name] = values[i]
            else:
                quote[str(indicators[i])] =  values[i]

        if self._get_format("", "obj") == "obj":
            return utils.to_obj("Quote", quote)
        else:
            return quote

    def _on_rpc_callback(self, method, data):
        print "_on_rpc_callback:", method, data

        if not self._callback:
            return

        try:
            if method == "jsq.quote_ind":
                q = self._convert_quote_ind(data)
                if q :
                    self._callback("quote", q)

        except Exception as e:
            print "Can't load jrpc", e.message

#    def login(self, username, password, format=""):
#        self._username = username
#        self._password = password
#        return self._do_login(format=format)
#
#    def _do_login(self, format=""):
#        # Shouldn't check connected flag here. ZMQ is a mesageq queue!
#        # if !self._connected :
#        #    return (False, "-1,no connection")
#
#        if self._username and self._password:
#            rpc_params = { "username" : self._username,
#                           "password" : self._password }
#
#            cr = self._remote.call("auth.login", rpc_params)
#            f = self._get_format(format, "")
#            if f != "obj" and f != "":
#                f = ""
#            return utils.extract_result(cr, format=f, class_name="UserInfo")
#        else:
#            return (False, "-1,empty username or password")
#        
#    def logout(self):
#        rpc_params = { }
#    
#        cr = self._remote.call("auth.logout", rpc_params)
#        return utils.extract_result(cr)

    def _call_rpc(self, method, data_format, data_class, **kwargs):

        r, msg = self._check_session()
        if not r:
            return (r, msg)

        rpc_params = { }
        for kw in kwargs.items():
            rpc_params[ str(kw[0]) ] = kw[1]

        cr = self._remote.call(method, rpc_params)
        
        return utils.extract_result(cr, data_format=data_format, class_name=data_class)

    def quote(self, security, fields="", data_format="", **kwargs):
        
        r, msg = self._call_rpc("jsq.query",
                                self._get_format(data_format, "obj"),
                                "Quote",
                                security = str(security),
                                fields   = fields,
                                **kwargs)
        return (r, msg)    

    def jsq_sub(self, security, fields="", func=None, data_format=""):
        """Subscribe securites
        
        This function adds new securities to subscribed list on the server. If
        success, return subscribed codes.
        
        If securities is empty, return current subscribed codes.
        """
        r, msg = self._check_session()
        if not r:
            return (r, msg)

        if func:
            self._on_jsq_callback = func
        
        rpc_params = {"security" : security,
                      "fields"   : fields }

        cr = self._remote.call("jsq.subscribe", rpc_params)
        
        rsp, msg = utils.extract_result(cr, data_format="obj", class_name="SubRsp")
        if not rsp:
            return (rsp, msg)

        self._schema_id = rsp.schema_id
        self._schema    = rsp.schema
        return (rsp.securities, msg)
        

    def jsq_unsub(self, security):
        """Unsubscribe securities.

        Unscribe codes and return list of subscribed code.
        """
        assert False, "NOT IMPLEMENTED"


    def bar(self, security, fields="", begin_time=200000, end_time=160000, 
        trade_date=0, cycle="1m", data_format="", **kwargs ) :

        return self._call_rpc("jsi.query",
                              self._get_format(data_format, "pandas"),
                              "Bar",
                              security   = str(security),
                              fields     = fields,
                              cycle      = cycle,
                              trade_date = trade_date,
                              begin_time = begin_time,
                              end_time   = end_time,
                              **kwargs)


    def daily(self, security, begin_date=0, end_date=0, 
        fields="",  adjust_mode = None, 
        data_format="", **kwargs ) :

        if adjust_mode == None:
            adjust_mode = "none"

        return self._call_rpc("jsd.query",
                              self._get_format(data_format, "pandas"),
                              "Daily",
                              security       = str(security),
                              fields         = fields,
                              begin_date     = begin_date,
                              end_date       = end_date,
                              adjust_mode    = adjust_mode,                             
                              **kwargs)

    def query(self, view, fields="", filter="", data_format="", **kwargs ) :
        return self._call_rpc( "jset.query",
                               self._get_format(data_format, "pandas"),
                               "JSetData",
                               view   = view,
                               fields = fields,
                               filter = filter,
                               **kwargs)

    def set_heartbeat(self, interval, timeout):
        self._remote.set_hearbeat_options(interval, timeout)


