# shell问题

### Q1.shell脚本是什么、它是必需的吗？  
```
一个Shell脚本是一个文本文件，包含一个或多个命令。作为系统管理员，我们经常需要使用多个命令来完成一项任务，我们可以添加这些所有命令在一个文本文件(Shell脚本)来完成这些日常工作任务。
```

### 2. for循环检测脚本
```
for i in `seq 1 1000`
do
        supervisorctl status | grep STAR
        sleep 1
done
```
