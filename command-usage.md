# some unusual shell command

### 1. watch 
```
watch ls /home/cento/opt/elasticsearch/bin/
```

### 2. for循环检测脚本
```
for i in `seq 1 1000`
do
        supervisorctl status | grep STAR
        sleep 1
done
```
