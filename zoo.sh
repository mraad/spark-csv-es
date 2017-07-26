#!/usr/bin/env bash

DAEMONS="\
 zookeeper-server\
 hadoop-hdfs-datanode\
 hadoop-hdfs-namenode\
 hadoop-hdfs-secondarynamenode\
 hadoop-httpfs\
 hadoop-yarn-nodemanager\
 hadoop-yarn-resourcemanager"

for daemon in ${DAEMONS}; do
    sudo service ${daemon} start
done
