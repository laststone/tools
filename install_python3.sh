#/bin/bash

#python只能向上兼容,如果该python包在centos6.5上打的,那它只能在6.5以上的系统中跑

cd /usr/local/src
wget https://www.python.org/ftp/python/3.5.2/Python-3.5.2.tgz
tar -zxvf Python-3.5.2.tgz
cd Python-3.5.2
./configure --prefix=/usr/local/src/python3.5
make && make install

# 然后安装所需要的模块,如pymysql requests等等

/usr/local/src/python3.5/bin/pip3.5 install requests
/usr/local/src/python3.5/bin/pip3.5 install pymysql

# 最后将python包打包到所需要的机器上解压即可
tar -zcvf python3.5.tar.gz /usr/local/src/python3.5


