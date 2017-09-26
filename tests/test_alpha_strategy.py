# -*- encoding: utf-8 -*-

from app.demoalphastrategy import DemoAlphaStrategy
from framework import common
from framework.gateway import DailyStockSimGateway
from framework.dataserver import JzDataServer
from framework.alphabacktest import AlphaBacktestInstance
import json


if __name__ == "__main__":
    prop_file_path = r"../etc/alpha.json"
    prop_file = open(prop_file_path, 'r')

    props = json.load(prop_file)

    enum_props = {}
    for k, v in enum_props.iteritems():
        props[k] = v.to_enum(props[k])

    strategy = DemoAlphaStrategy()
    gateway = DailyStockSimGateway()
    data_server = JzDataServer()

    backtest = AlphaBacktestInstance()
    backtest.init_from_config(props, data_server, gateway, strategy)

    backtest.run_alpha()
