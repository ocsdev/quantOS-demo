# -*- encoding: utf-8 -*-

import json

from app.demoalphastrategy import DemoAlphaStrategy
from framework.alphabacktest import AlphaBacktestInstance
from framework.dataserver import JzDataServer
from framework.gateway import DailyStockSimGateway

if __name__ == "__main__":
    import os
    import time
    
    prop_file_path = r"../etc/alpha.json"
    print os.path.abspath(prop_file_path)
    prop_file = open(prop_file_path, 'r')
    
    props = json.load(prop_file)
    
    enum_props = {}
    for k, v in enum_props.iteritems():
        props[k] = v.to_enum(props[k])
    
    strategy = DemoAlphaStrategy()
    gateway = DailyStockSimGateway()
    data_server = JzDataServer()
    
    backtest = AlphaBacktestInstance()
    t_start = time.time()
    backtest.init_from_config(props, data_server, gateway, strategy)
    t1 = time.time() - t_start
    print "[init from config] time lapsed in total: {:.2f}".format(t1)
    
    backtest.run_alpha()
    t2 = time.time() - t_start
    print "[run backtest] time lapsed in total: {:.2f}".format(t2)
    
    backtest.save_results('../output/')
    t3 = time.time() - t_start
    print "[backtest] time lapsed in total: {:.2f}".format(t3)
