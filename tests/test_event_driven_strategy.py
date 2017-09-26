# -*- encoding: utf-8 -*-

import json

from framework import *

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
    data_server = JshHistoryBarDataServer()
    
    backtest = BacktestInstance()
    backtest.initFromConfig(props, data_server, gateway, strategy)
    
    # backtest.run()
    backtest.run2()
    report = backtest.generateReport()
    # print report.trades[:100]
    # for pnl in report.daily_pnls:
    #     print pnl.date, pnl.trade_pnl, pnl.hold_pnl,pnl.total_pnl, pnl.positions.get('600030.SH')
