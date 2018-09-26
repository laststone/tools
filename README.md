# some test exercise
#### aggregate opentsdb data (1point/2hours)
```
(1) config the config.ini
(2) run test 
     cd /tsdb-aggregate/huyang/data_aggregate/aggregate && /usr/local/src/python2.7/bin/python2.7 aggregate.py
(3) add to the crontab task
     0 0 */1 * * cd /tsdb-aggregate/huyang/data_aggregate/aggregate && /usr/local/src/python2.7/bin/python2.7 aggregate.py
 
```
show the aggregated dashbord
![](./img/dashboard.png)