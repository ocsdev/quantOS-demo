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
    
    def init_from_config(self, props, data_server, gateway, strategy):
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
            # recorder.record_info()
            # self.last_date = self.current_date
            
            if not gateway.match_finished:
                self.current_date = self.calendar.get_next_trade_date(self.current_date)
            else:
                self.current_date = self.calendar.get_next_period_day(self.current_date,
                                                                      self.strategy.period, self.strategy.days_delay)
            
            # self.last_date = self.calendar.get_last_trade_date(self.current_date)
            self.strategy.on_new_day(self.current_date)
            gateway.on_new_day(self.current_date)
            
            if not gateway.match_finished:
                # TODO here we must make sure the matching will not last to next period
                
                df_dic = self.strategy.get_univ_prices()
                trade_indications = gateway.match(df_dic, self.current_date)
                for trade_ind in trade_indications:
                    gateway.on_trade_ind(trade_ind)
            else:
                # suspension_list = data_server.get_suspensions(strategy.context.universe, current_date)
                suspension_list = None
                
                self.strategy.re_balance(suspension_list)
                
                # TODO settle function


if __name__ == "__main__":
    from alphastrategy import AlphaStrategy
    
    strategy = AlphaStrategy()
    gateway = strategy.context.gateway
