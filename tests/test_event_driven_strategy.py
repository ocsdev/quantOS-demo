# -*- encoding: utf-8 -*-

import json
import os

from quantos import SOURCE_ROOT_DIR
from quantos.backtest import model
from quantos.backtest import common
from quantos.data.dataserver import JzEventServer
from quantos.example.doubleMaStrategy import DoubleMaStrategy
from quantos.backtest.backtest import EventBacktestInstance
from quantos.backtest.gateway import BarSimulatorGateway

from quantos.backtest import *

if __name__ == "__main__":
    
    prop_file_path = os.path.join(SOURCE_ROOT_DIR, "etc/backtest.json")
    print prop_file_path
    prop_file = open(prop_file_path, 'r')
    
    props = json.load(prop_file)
    
    enum_props = {'bar_type': common.QUOTE_TYPE}
    for k, v in enum_props.iteritems():
        props[k] = v.to_enum(props[k])
    
    # strategy   = CtaStrategy()
    strategy = DoubleMaStrategy()
    gateway = BarSimulatorGateway()
    data_server = JzEventServer()

    context = model.Context()
    context.register_data_api(data_server)
    context.register_gateway(gateway)
    context.register_trade_api(gateway)
    
    backtest = EventBacktestInstance()
    backtest.init_from_config(props, strategy, context=context,
                              data_api=data_server, gateway=gateway, dataview=None)
    
    # backtest.run()
    backtest.run2()
    report = backtest.generate_report()
    # print report.trades[:100]
    # for pnl in report.daily_pnls:
    #     print pnl.date, pnl.trade_pnl, pnl.hold_pnl,pnl.total_pnl, pnl.positions.get('600030.SH')
