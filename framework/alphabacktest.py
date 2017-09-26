# encoding: utf-8

from framework import common
from framework.jzcalendar import JzCalendar
from pubsub import Subscriber


class BacktestInstance(Subscriber):
    def __init__(self):
        Subscriber.__init__(self)
        
        self.instanceid = ''
        self.strategy = None
        self.start_date = 0
        self.end_date = 0
        self.current_date = 0
        self.last_date = 0
        self.folder = ''
        
        self.calendar = JzCalendar()
        
        self.props = None
    
    def init_from_config(self, props, data_server, gateway, strategy):
        self.props = props
        self.instanceid = props.get("instanceid")
        
        self.start_date = props.get("start_date")
        self.end_date = props.get("end_date")
        
        data_server.init_from_config(props)
        data_server.initialize()
        
        gateway.init_from_config(props)
        
        strategy.context.data_server = data_server
        strategy.context.gateway = gateway
        strategy.context.calendar = self.calendar
        
        strategy.init_from_config(props)
        strategy.initialize(common.RUN_MODE.BACKTEST)
        
        self.strategy = strategy
        
        return True


class AlphaBacktestInstance(BacktestInstance):
    """
    Attributes
    ----------
    strategy : AlphaStrategy
    
    """
    def __init__(self):
        BacktestInstance.__init__(self)
        pass
    
    def run_alpha(self):
        data_server = self.strategy.context.data_server
        universe = self.strategy.context.universe
        gateway = self.strategy.context.gateway
        
        data_server.add_batch_subscribe(self, universe)
        
        self.current_date = self.start_date
        while self.current_date <= self.end_date:
            if gateway.match_finished:
                self.current_date = self.calendar.get_next_period_day(self.current_date,
                                                                      self.strategy.period, self.strategy.days_delay)
                self.last_date = self.calendar.get_last_trade_date(self.current_date)

                self.on_new_day(self.last_date)
                self.strategy.re_balance()

                self.on_new_day(self.current_date)
                self.strategy.send_bullets()
            else:
                # TODO here we must make sure the matching will not last to next period
                self.current_date = self.calendar.get_next_trade_date(self.current_date)
                self.on_new_day(self.current_date)
                
            df_dic = self.strategy.get_univ_prices()
            trade_indications = gateway.match(df_dic, self.current_date)
            for trade_ind in trade_indications:
                gateway.on_trade_ind(trade_ind)
                
        print "Backtest done."
        self.save_results()
    
    def on_new_day(self, date):
        self.strategy.on_new_day(date)
        self.strategy.context.gateway.on_new_day(date)
    
    def save_results(self, folder='../output/'):
        import pandas as pd
        import json
        
        trades = self.strategy.pm.trades
        
        type_map = {'task_id': str,
                    'entrust_no': str,
                    'entrust_action': str,
                    'security': str,
                    'fill_price': float,
                    'fill_size': int,
                    'fill_date': int,
                    'fill_time': int,
                    'fill_no': str}
        # keys = trades[0].__dict__.keys()
        ser_list = dict()
        for key in type_map.keys():
            v = [t.__getattribute__(key) for t in trades]
            ser = pd.Series(data=v, index=None, dtype=type_map[key], name=key)
            ser_list[key] = ser
        df_trades = pd.DataFrame(ser_list)
        
        df_trades.to_csv(folder+'trades.csv')
        
        json.dump(self.props, open(folder+'configs.json', 'w'))


if __name__ == "__main__":
    from alphastrategy import AlphaStrategy
    
    strategy = AlphaStrategy()
    gateway = strategy.context.gateway
