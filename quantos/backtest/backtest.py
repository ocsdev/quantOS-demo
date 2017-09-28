# encoding: utf-8

import quantos.util.fileio
from event.eventType import EVENT
from quantos.backtest import common
from quantos.backtest.analyze.pnlreport import PnlManager
from quantos.backtest.calendar import Calendar
from quantos.backtest.event.eventEngine import Event

from quantos.backtest.pubsub import Subscriber


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
        
        self.calendar = Calendar()
        
        self.props = None
        
        self.context = None
    
    def init_from_config(self, props, strategy, data_api=None, dataview=None, gateway=None, context=None):
        """
        
        Parameters
        ----------
        props
        strategy
        data_api
        dataview
        gateway
        context : Context

        Returns
        -------

        """
        self.props = props
        self.instanceid = props.get("instanceid")
        
        self.start_date = props.get("start_date")
        self.end_date = props.get("end_date")
        
        self.context = context
        # TODO
        self.context.add_universe(props['universe'])
        
        strategy.context = self.context
        # strategy.context.data_api = data_api
        # strategy.context.gateway = gateway
        # strategy.context.calendar = self.calendar
        
        strategy.init_from_config(props)
        strategy.initialize(common.RUN_MODE.BACKTEST)
        
        self.strategy = strategy
        
        return True


class AlphaBacktestInstance(BacktestInstance):
    """
    Attributes
    ----------
    
    """
    def __init__(self):
        BacktestInstance.__init__(self)
        
        self.trade_days = None
    
    def _is_trade_date(self, start, end, date, data_server):
        if self.trade_days is None:
            df, msg = data_server.daily('000300.SH', start, end, fields="close")
            self.trade_days = df.loc[:, 'trade_date'].values
        return date in self.trade_days
    
    def go_next_trade_date(self):
        if self.context.gateway.match_finished:
            self.current_date = self.calendar.get_next_period_day(self.current_date,
                                                                  self.strategy.period, self.strategy.days_delay)
            self.last_date = self.calendar.get_last_trade_date(self.current_date)
        else:
            # TODO here we must make sure the matching will not last to next period
            self.current_date = self.calendar.get_next_trade_date(self.current_date)
        
        while (self.current_date < self.end_date and
                   not self._is_trade_date(self.start_date, self.end_date, self.current_date,
                                           self.context.data_api)):
            self.current_date = self.calendar.get_next_trade_date(self.current_date)
    
    def run_alpha(self):
        gateway = self.context.gateway
        
        self.current_date = self.start_date
        while True:
            self.go_next_trade_date()
            if self.current_date > self.end_date:
                break
            
            if gateway.match_finished:
                self.on_new_day(self.last_date)
                df_dic = self.strategy.get_univ_prices()  # access data
                self.strategy.re_balance_plan(df_dic, suspensions=[])
                
                self.on_new_day(self.current_date)
                self.strategy.send_bullets()
            else:
                self.on_new_day(self.current_date)
            
            df_dic = self.strategy.get_univ_prices()  # access data
            trade_indications = gateway.match(df_dic, self.current_date)
            for trade_ind in trade_indications:
                gateway.on_trade_ind(trade_ind)
        
        print "Backtest done. {:d} days, {:.2e} trades in total.".format(len(self.trade_days),
                                                                         len(self.strategy.pm.trades))
    
    def on_new_day(self, date):
        self.strategy.on_new_day(date)
        self.context.gateway.on_new_day(date)
    
    def save_results(self, folder='../output/'):
        import pandas as pd
        
        trades = self.strategy.pm.trades
        
        type_map = {'task_id': str,
                    'entrust_no': str,
                    'entrust_action': str,
                    'symbol': str,
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
        
        df_trades.to_csv(folder + 'trades.csv')
        
        quantos.util.fileio.save_json(self.props, folder + 'configs.json')


class AlphaBacktestInstance2(AlphaBacktestInstance):
    def run_alpha(self):
        gateway = self.context.gateway
        
        self.current_date = self.start_date
        while True:
            self.go_next_trade_date()
            if self.current_date > self.end_date:
                break
            
            if gateway.match_finished:
                self.on_new_day(self.last_date)
                df_dic = self.get_univ_prices(field_name='close')  # access data
                self.strategy.re_balance_plan(df_dic, self.get_suspensions())
                
                self.on_new_day(self.current_date)
                self.strategy.send_bullets()
            else:
                self.on_new_day(self.current_date)
            
            df_dic = self.get_univ_prices(field_name="close,vwap,open,high,low")  # access data
            trade_indications = gateway.match(df_dic, self.current_date)
            for trade_ind in trade_indications:
                gateway.on_trade_ind(trade_ind)
        
        print "Backtest done. {:d} days, {:.2e} trades in total.".format(len(self.context.dataview.dates),
                                                                         len(self.strategy.pm.trades))
        
    def get_univ_prices(self, field_name='close'):
        dv = self.context.dataview
        df = dv.get_snapshot(self.current_date, fields=field_name)
        gp = df.groupby(by='symbol')
        return {sec: df for sec, df in gp}
    
    def _is_trade_date(self, start, end, date, data_server):
        return date in self.context.dataview.dates
    
    def get_suspensions(self):
        trade_status = self.context.dataview.get_snapshot(self.current_date, fields='trade_status')
        trade_status = trade_status.loc[:, 'trade_status']
        mask_sus = trade_status != u'交易'.encode('utf-8')
        return list(trade_status.loc[mask_sus].index.values)
    



class EventBacktestInstance(BacktestInstance):
    def __init__(self):
        super(EventBacktestInstance, self).__init__()
        
        self.pnlmgr = None

    def init_from_config(self, props, strategy, data_api=None, dataview=None, gateway=None, context=None):
        self.props = props
        self.instanceid = props.get("instanceid")

        self.start_date = props.get("start_date")
        self.end_date = props.get("end_date")

        data_api.init_from_config(props)
        data_api.initialize()

        gateway.init_from_config(props)

        self.context = context
        strategy.context = self.context
        # strategy.context.dataserver = data_api
        # strategy.context.gateway = gateway
        # strategy.context.calendar = self.calendar

        gateway.register_callback(strategy.pm)

        strategy.init_from_config(props)
        strategy.initialize(common.RUN_MODE.BACKTEST)

        self.strategy = strategy

        self.pnlmgr = PnlManager()
        self.pnlmgr.setStrategy(strategy)
        self.pnlmgr.initFromConfig(props, data_api)

        return True
    
    def run(self):
        
        data_server = self.context.dataserver
        universe = self.context.universe
        
        data_server.add_batch_subscribe(self, universe)
        
        last_trade_date = 0
        while (True):
            quote = data_server.getNextQuote()
            
            if quote is None:
                break
            
            trade_date = quote.getDate()
            
            # switch to a new day
            if (trade_date != last_trade_date):
                
                if (last_trade_date > 0):
                    self.close_day(last_trade_date)
                
                self.strategy.trade_date = trade_date
                self.strategy.pm.on_new_day(trade_date, last_trade_date)
                self.strategy.onNewday(trade_date)
                last_trade_date = trade_date
            
            self.process_quote(quote)
    
    def get_next_trade_date(self, current):
        next_dt = self.calendar.get_next_trade_date(current)
        return next_dt
    
    def run2(self):
        data_api = self.context.data_api
        universe = self.context.universe
        
        data_api.add_batch_subscribe(self, universe)
        
        self.current_date = self.start_date
        
        # ------------
        def __extract(func):
            return lambda event: func(event.data, **event.kwargs)
        
        ee = self.strategy.eventEngine  # TODO event-driven way of lopping, is it proper?
        ee.register(EVENT.CALENDAR_NEW_TRADE_DATE, __extract(self.strategy.onNewday))
        ee.register(EVENT.MD_QUOTE, __extract(self.process_quote))
        ee.register(EVENT.MARKET_CLOSE, __extract(self.close_day))
        
        # ------------
        
        while self.current_date <= self.end_date:  # each loop is a new trading day
            quotes = data_api.get_daily_quotes(self.current_date)
            if quotes is not None:
                # gateway.oneNewDay()
                e_newday = Event(EVENT.CALENDAR_NEW_TRADE_DATE)
                e_newday.data = self.current_date
                ee.put(e_newday)
                ee.process_once()  # this line should be done on another thread
                
                # self.strategy.onNewday(self.current_date)
                self.strategy.pm.on_new_day(self.current_date, self.last_date)
                self.strategy.trade_date = self.current_date
                
                for quote in quotes:
                    # self.processQuote(quote)
                    e_quote = Event(EVENT.MD_QUOTE)
                    e_quote.data = quote
                    ee.put(e_quote)
                    ee.process_once()
                
                # self.strategy.onMarketClose()
                # self.closeDay(self.current_date)
                e_close = Event(EVENT.MARKET_CLOSE)
                e_close.data = self.current_date
                ee.put(e_close)
                ee.process_once()
                # self.strategy.onSettle()
                
                self.last_date = self.current_date
            else:
                # no quotes because of holiday or other issues. We don't update last_date
                print "in backtest.py: function run(): {} quotes is None, continue.".format(self.last_date)
            
            self.current_date = self.get_next_trade_date(self.current_date)
            
            # self.strategy.onTradingEnd()
    
    def process_quote(self, quote):
        result = self.context.gateway.process_quote(quote)
        
        for (tradeInd, statusInd) in result:
            self.strategy.pm.on_trade_ind(tradeInd)
            self.strategy.pm.on_order_status(statusInd)
        
        self.strategy.onQuote(quote)

    # close one trade day, cancel all orders
    def close_day(self, trade_date):
        print 'close trade_date ' + str(trade_date)
        result = self.context.gateway.close_day(trade_date)
        
        for statusInd in result:
            self.strategy.pm.on_order_status(statusInd)
    
    def generate_report(self):
        return self.pnlmgr.generateReport()
