# encoding: UTF-8
from enum import Enum, unique


class ReprEnum(Enum):
    def __repr__(self):
        return "{0:s}_{1:s}".format(self.__class__.__name__,
                                    self._name_)

    def __str__(self):
        return "{0:s}_{1:s}".format(self.__class__.__name__,
                                    self._name_)

    @property
    def full_name(self):
        return str(self)

    @classmethod
    def to_enum(cls, key):
        return cls.__members__[key]


class ReprIntEnum(int, ReprEnum):
    """Enum where members are also (and must be) ints"""
    pass


class ReprStrEnum(str, ReprEnum):
    """Enum where members are also (and must be) ints"""
    pass


@unique
class QUOTE_TYPE(ReprIntEnum):
    TICK = 0
    MINBAR = 1
    FIVEMINBAR = 5
    QUARTERBAR = 15
    DAILY = 1440
    SPECIALBAR = -1


@unique
class RUN_MODE(ReprIntEnum):
    REALTIME = 0
    BACKTEST = 1


@unique
class ORDER_TYPE(ReprStrEnum):
    LIMITORDER = "LimitOrder"
    STOPORDER = "StopOrder"


@unique
class ORDER_ACTION(ReprStrEnum):
    BUY = "Buy"
    SELL = "Sell"
    SHORT = "Short"
    COVER = "Cover"
    SELLTODAY = "SellToday"
    SELLYESTERDAY = "SellYesterday"
    COVERYESTERDAY = "CoverYesterday"
    COVERTODAY = "CoverToday"


@unique
class ORDER_STATUS(ReprStrEnum):
    NEW = "New"
    ACCEPTED = "Accepted"
    FILLED = "Filled"
    CANCELLED = "Cancelled"
    REJECTED = "Rejected"


"""
RUNMODE_REALTIME = 0
RUNMODE_BACKTEST = 1

QUOTE_TYPE_TICK       = 0    
QUOTE_TYPE_MINBAR     = 1    
QUOTE_TYPE_FiVEMINBAR = 5    
QUOTE_TYPE_QUARTERBAR = 15   
QUOTE_TYPE_DAILY      = 1440
QUOTE_TYPE_SPECIALBAR = -1

ORDER_TYPE_LIMITORDER        = "LimitOrder"
ORDER_TYPE_STOPORDER         = "StopOrder"

ORDER_ACTION_BUY             = "Buy"                      
ORDER_ACTION_SELL            = "Sell"                     
ORDER_ACTION_SHORT           = "Short"                    
ORDER_ACTION_COVER           = "Cover"                    
ORDER_ACTION_SELLTODAY       = "SellToday"       
ORDER_ACTION_SELLYESTERDAY   = "SellYesterday"   
ORDER_ACTION_COVERYESTERDAY  = "CoverYesterday"  
ORDER_ACTION_COVERTODAY      = "CoverToday"      
                                                  
ORDER_STATUS_NEW       = "New"                   
ORDER_STATUS_ACCEPTED  = "Accepted"              
ORDER_STATUS_FILLED    = "Filled"                
ORDER_STATUS_CANCELLED = "Cancelled"             
ORDER_STATUS_REJECTED  = "Rejected"              
"""


if __name__ == "__main__":
    """What below are actually unit tests. """
    print QUOTE_TYPE.TICK == 0
    print RUN_MODE.BACKTEST == 1
    print ORDER_ACTION.BUY == 'Buy'
