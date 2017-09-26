# -*- encoding: utf-8 -*-

import json

from quantos.data.dataserver import JzEventServer
from quantos.example.doubleMaStrategy import DoubleMaStrategy

from quantos.backtest import *

if __name__ == "__main__":
    prop_file_path = r"../etc/backtest.json"
    prop_file = open(prop_file_path, 'r')
    
    props = json.load(prop_file)
    
    enum_props = {'bar_type': common.QUOTE_TYPE}
    for k, v in enum_props.iteritems():
        props[k] = v.to_enum(props[k])
    
    # strategy   = CtaStrategy()
    strategy = DoubleMaStrategy()
    gateway = BarSimulatorGateway()
    data_server = JzEventServer()
    
    backtest = EventBacktestInstance()
    backtest.init_from_config(props, data_server, gateway, strategy)
    
    # backtest.run()
    backtest.run2()
    report = backtest.generate_report()
    # print report.trades[:100]
    # for pnl in report.daily_pnls:
    #     print pnl.date, pnl.trade_pnl, pnl.hold_pnl,pnl.total_pnl, pnl.positions.get('600030.SH')
