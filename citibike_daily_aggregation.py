#This Program will aggreagate Previous day retrived files from HDFS in Aggregated data directory 
# and move these files to Daily Backup Data directory

#Author Shailesh and Mohammad

import os
import subprocess
import pandas as pd
import findspark
findspark.init()

import pyspark
import random
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql import SQLContext

from hdfs import InsecureClient
from datetime import date, timedelta


yesterday = date.today() - timedelta(1)
date = yesterday.strftime('%Y%m%d')

sc = pyspark.SparkContext(appName="Citibike_aggregation_"+date)

csvDf = sqlContext.read.format("csv").option("header", "true").option("inferschema", "true").option("mode", "DROPMALFORMED").load('hdfs://ubuntuclient2.psudata.dev:8020/user/hdfs/citibike/raw_data/'+date+"*.csv")
pd_df=csvDf.toPandas()
with client_hdfs.write('/user/hdfs/citibike/daily_aggregated_data/aggregated_'+date+'.csv' ,encoding = 'utf-8') as writer:
    pd_df.to_csv(writer,index=False)
mkdir_command = 'hdfs dfs -mkdir /user/hdfs/citibike/daily_backup_data/'+date
cmd_mkdir = mkdir_command.split()
subprocess.check_output(cmd_mkdir)
    
command_mv = 'hdfs dfs -mv /user/hdfs/citibike/raw_data/'+date+'*.csv  /user/hdfs/citibike/daily_backup_data/'+date
cmd_mv = command_mv.split()
subprocess.check_output(cmd_mv)   

sc.stop()

