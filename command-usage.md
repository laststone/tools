# some unusual shell command

### 1. watch 
```
watch -n 5 ls /home/cento/opt/elasticsearch/bin/
```

### 2. for循环检测脚本
```
for i in `seq 1 1000`
do
        supervisorctl status | grep STAR
        sleep 1
done
```

### 3.AWS主机磁盘分区/格式化/挂载/自动挂载步骤
```
fdik -l                                # 查看到未分区的那块盘/dev/xvda, 其上已经划分了一个主分区(系统安装分区)
disk /dev/xvda --> n(新建分区) --> p(新建主分区) --> 2(主分区号2) --> 默认起始位置 --> 默认终止位置(使用全部未使用空间或+500G指定大小) --> w (保存分区退出)
reboot                                 # 重启系统使/dev/xvda2新分区设备出现
/sbin/mkfs.ext4 -L /data /dev/xvda2    # -L lable标签选项,格式化磁盘设备
mount /dev/xvda2 /data                 # 挂载分区
vi /etc/fstab                          # 设置开启自动挂载
        LABEL=/data             /data                   ext4    defaults        1 2

```

### 4.unzip -qq 静默解压
```
unzip -qq xx.zip
```

### 5.top + 1 查看CPU的核心数
```
top → 1
```
