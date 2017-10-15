# User Guide

本页面给用户提供了一个简洁清晰的入门指南，包括从安装到代码调用的用法，更多细节请参考[数据文档]()

## DataApi 

本产品提供了金融数据api，方便用户调用接口获取各种数据，通过python的api调用接口，返回DataFrame格式的数据和消息，以下是用法

### 引入API

#### 导入接口
```python
import data_api
from data_api import DataApi
```
#### 登录数据服务器
```python
api = DataApi("tcp://140.207.224.19:8910")
api.login("demo", "666666") # 示例账户，用户需要改为自己注册的账户
```

### 数据分为两大部分：
- **市场数据**，目前可使用的数据包括日线，分钟线
- **参考数据**，包括财务数据、公司行为数据、指数成份数据等

### 市场数据获取

#### 获取日线行情
代码示例：
```python
df, msg = api.daily(
                symbol="600832.SH, 600030.SH", 
                start_date="2012-10-26",
                end_date="2012-11-30", 
                fields="", 
                adjust_mode="post")
```
<!-- 结果示例：

|close | code | high	| low	| oi	| open	| settle	| symbol	|trade_date	|trade_status	|turnover	|volume	|vwap|
| --- | --- | --- |--- |--- |--- |--- |--- |--- |--- |--- |--- |--- |
|5.09|	600832|	5.24|	5.08|	NaN	|5.23	|NaN	|600832.SH|	20121026	|交易	|2.779057e+07|	5381800	5.16|
|5.10|	600832|	5.15|	5.08|	NaN	|5.11	|NaN	|600832.SH|	20121029	|交易	|1.320333e+07|	2582557	5.11|
|5.11|	600832|	5.18|	5.08|	NaN	|5.12	|NaN	|600832.SH|	20121030	|交易	|1.622705e+07|	3170615	5.12|
|5.11|	600832|	5.14|	5.09|	NaN	|5.12	|NaN	|600832.SH|	20121031	|交易	|1.072007e+07|	2097770	5.11|
|5.18|	600832|	5.20|	5.12|	NaN	|5.12	|NaN	|600832.SH|	20121101	|交易	|1.972100e+07|	3814712	5.17| -->

#### 获取分钟线行情（不含ask，bid信息）
代码示例：
```python
df,msg = api.bar(
            symbol="600030.SH", 
            trade_date=20170928, 
            freq="5m",
            start_time="00:00:00",
            end_time="16:00:00",
            fields="")
```
<!-- 结果示例：

|close	|code|	date	|  freq	|high	|low	|oi	|open	|settle	|symbol	|time|	trade_date|	turnover	|volume	|vwap|
| --- | --- | --- |--- |--- |--- |--- |--- |--- |--- |--- |--- |--- |--- |--- |
|18.05|	600030	|20170928	|5m	|18.08	|18.00|	NaN|	18.01|	NaN|	600030.SH	|93500|	20170928	|13576973.0|	752900	|18.032903|
|18.03|	600030	|20170928	|5m	|18.06	|18.01|	NaN|	18.04|	NaN|	600030.SH	|94000|	20170928	|16145566.0|	895110	|18.037522|
|18.04|	600030	|20170928	|5m	|18.05	|18.02|	NaN|	18.03|	NaN|	600030.SH	|94500|	20170928	|11024829.0|	611400	|18.032105|
|17.99|	600030	|20170928	|5m	|18.05	|17.97|	NaN|	18.04|	NaN|	600030.SH	|95000|	20170928	|30021003.0|	1667190 |18.006948|
|18.02|	600030	|20170928	|5m	|18.03	|17.97|	NaN|	17.98|	NaN|	600030.SH	|95500|	20170928	|13691203.0|	761161	|17.987263| -->



#### 获取分钟线行情（包含ask,bid信息）
代码示例：
```python
df,msg = api.bar_quote(
                    symbol="000001.SH,cu1709.SHF",  
                    start_time = "09:56:00", 
                    end_time="13:56:00", 
                    trade_date=20170823, 
                    freq= "5m",
                    fields="open,high,low,last,volume")
```
<!-- 结果示例：

|high	|low	|symbol|	time	|trade_date|	volume|
|--- |--- |--- |--- |--- |--- |
|3294.3371	|3291.7666	|000001.SH	|100000	|20170823	|493058300|
|3292.3162	|3289.5202	|000001.SH	|100500	|20170823	|492695100|
|3290.4118	|3288.3906	|000001.SH	|101000	|20170823	|458298100|
|3289.2133	|3285.9129	|000001.SH	|101500	|20170823	|535085000|
|3287.4892	|3284.6076	|000001.SH	|102000	|20170823	|426738700| -->

### 基本数据获取

#### 获取证券基础信息
代码示例：
```python
df, msg = api.query(
                view="lb.instrumentInfo", 
                fields="status,list_date, fullname_en, market", 
                filter="inst_type=&status=1&symbol=", 
                orderby="symbol, -market",
                data_format='pandas')
```

#### 获取交易日历
代码示例：
```python
df, msg = api.query(
                view="jz.secTradeCal", 
                fields="date,market,istradeday,isweekday,isholiday", 
                filter="market=1&start_date=20170101&end_date=20170801", 
                data_format='pandas')
```

#### 获取分配除权信息
代码示例：
```python
df, msg = api.query(
                view="lb.secDividend", 
                fields="", 
                filter="start_date=20170101&end_date=20170801", 
                data_format='pandas')
```

#### 获取复权因子
代码示例：
```python
df, msg = api.query(
                view="lb.secAdjFactor", 
                fields="", 
                filter="symbol=002059&start_date=20170101&end_date=20170801", 
                data_format='pandas')
```

#### 获取停牌信息
代码示例：
```python
df, msg = api.query(
                view="lb.secSusp", 
                fields="susp_time", 
                filter="symbol=002059", 
                data_format='pandas')
```

#### 获取行业分类
代码示例：
```python
df, msg = api.query(
                view="lb.secIndustry", 
                fields="", 
                filter="industry1_name=金融&industry2_name=金融&industry_src=中证", 
                data_format='pandas')
```

#### 获取指数成份
代码示例：
```python
df, msg = api.query(
                view="lb.indexCons", 
                fields="", 
                filter="index_code=399001&is_new=Y", 
                data_format='pandas')
```

#### 获取常量参数
代码示例：
```python
df, msg = api.query(
                view="jz.sysConstants", 
                fields="", 
                filter="code_type=symbol_type", 
                data_format='pandas'
                )
```

#### 获取日行情估值
代码示例：
```python
df, msg = api.query(
                view="wd.secDailyIndicator",
                fields='pb,net_assets,ncf,price_level',
                filter='symbol=000063.SZ&start_date=20170605&end_date=20170701',
                orderby="trade_date"
                )
```
#### 获取资产负债表
代码示例：
```python
df, msg = api.query(
                view="lb.balanceSheet", 
                fields="", 
                filter="symbol=002636.SZ", 
                orderby="ann_date",
                data_format='pandas')
```

#### 获取利润表
代码示例：
```python
df, msg = api.query(
                view="lb.income", 
                fields="", 
                filter="symbol=600030.SH,000063.SZ,000001.SZ&report_type=408002000&start_date=20160601&end_date=20170601", 
                orderby="-report_date",
                data_format='pandas')
```

#### 获取现金流量表
代码示例：
```python
df, msg = api.query(
                view="lb.cashFlow", 
                fields="", 
                filter="symbol=002548.SZ", 
                orderby="",
                data_format='pandas')
```

#### 获取业绩快报
代码示例：
```python
df, msg = api.query(
                view="lb.profitExpress", 
                fields="", 
                filter="start_anndate=20170101", 
                orderby="-ann_date",
                data_format='pandas')
```

#### 获取限售股解禁表
代码示例：
```python
df, msg = api.query(
                view="lb.secRestricted", 
                fields="", 
                filter="list_date=20170925", 
                orderby="",
                data_format='pandas')
```

#### 获取财务指标
代码示例：
```python
df, msg = api.query(
                view="lb.finindicator", 
)
```

#### 获取指数基本信息
代码示例：
```python
df, msg = api.query(
                view="lb.indexInfo", 
                fields="", 
                filter="symbol=399001.SZ", 
                orderby="",
                data_format='pandas' 
                )
```

#### 获取指数成份股
代码示例：
```python
df, msg = api.query(
                view="lb.indexCons", 
                fields="", 
                filter="index_code=399001&is_new=Y", 
                data_format='pandas')
```


## Dataview

## Research

此处填写研究指南

## Strategy

## BackTest

此处填写回测指南