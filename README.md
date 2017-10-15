
# Welcome

在这里，你将可以获得：

- 使用数据API，轻松获取研究数据
- 使用信号研究工具，进行交易信号的快速定义、研究、回测
- 根据策略模板，编写自己的量化策略
- 使用回测框架，对策略进行回测和验证


# Dependencies

	scipy==0.19.0
	pandas==0.20.1
	numpy==1.12.1
	pytest==3.0.7
	Jinja2==2.9.6
	statsmodels==0.8.0
	matplotlib==2.0.2
	ipython==6.2.1
	msgpack_python==0.4.8
	nose_parameterized==0.6.0
	pypandoc==1.4
	seaborn==0.8.1
	setuptools==36.6.0
	six==1.11.0
	ctypes_snappy==1.03
	xarray==0.9.6
	pyzmq==17.0.0b1

	

# Installation

目前可以在如下操作系统上安装

-  GNU/Linux 64-bit
-  Windows 64-bit
-  Mac OS

如果还没有Python环境，建议先安装所对应操作系统的Python集成开发环境 [Anaconda](http://www.continuum.io/downloads "Anaconda")，再安装quantos。

安装方式主要有以下几种：

1、使用``pip``进行安装
-----------------------
    $ pip install quantos

2、通过源代码安装
--------
clone quantOS的源代码，进入到源文件目录，执行安装命令：
	
	$ python setup.py install
或者通过pypi地址https://pypi.python.org/quantOS-Org/quantos/下载,并执行上面安装命令。

3、代码升级
--------

	$ python install quantos --upgrade

# Quickstart

参见 [user guide](doc/user_guide.md "user guide")


更多的示例保存在 ``quantos/examples`` 

Questions?
==========

如果您发现任何问题，请到[这里](https://github.com/quantOSorg/quantos/issues/new)提交。


Change Logs
=========

0.2.0 2017/10/16
----
1、发布第一个版本

