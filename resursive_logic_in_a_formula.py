# script to apply recursive logic usinga  formula

import os
import sys
import pandas as pd
from datetime import datetime
from datetime import timedelta
#####library required for s3 operations######
import boto3
os.environ['SPARK_HOME'] = "/usr/lib/spark/"
sys.path.append("/usr/lib/spark/python/")
from pyspark.sql import SparkSession
from pyspark.sql.types import *


spark = SparkSession.builder.enableHiveSupport().appName("").getOrCreate()
spark.sql("")

spark_df= spark.sql("").toPandas()




def discont(x):
    restrt=0
    discont=0
    if x>0:
        if (x-11)>0:
            for m in range(1,x-11):
                restrt=restrt+restart(m-1)
        else:
            restrt=0
        for n in range(1,x):
            discont=discont+discont(n-1)
        discont=spark_df[''][x] * (spark_df[''][x] + restrt - discont)
    else:
        discont=0
    return discont

def restart(x):
    restrt=0
    discont=0
    if x>0:
        for m in range(1,x):
            restrt=restrt+restart(m-1)
        for n in range(1,x):
            discont=discont+discont(n-1)
        restrt=spark_df[''][x] * (discont-restrt)
    else:
        restrt=0
    return restrt


# logic : get values from previously calculated ---reiterate
def prev_value_discont(data,a):
    df=pd.DataFrame(data,columns=["",""])
    restrt=0
    discont=0
    for i in range(0,a-11):
        restrt=restrt+df[''][i]
    for i in range(0,a):
        discont=discont+df[''][i]
    discont=spark_df[''][a] * (spark_df[''][a] + restrt - discont)
    return discont




def prev_value_restart(data,a):
    df=pd.DataFrame(data,columns=["",""])
    restrt=0
    discont=0
    for i in range(0,a):
        restrt=restrt+df[''][i]
    for i in range(0,a):
        discont=discont+df[''][i]
    restrt=spark_df[''][a] * ()
    return restrt

#main
week=spark_df['num']
maxval=week.max()
startval=0
endval=20
data=[]
data1=[]
data2=[]


print("part 1")
for i in range(startval,endval):
    data1.append([discont(i),restart(i)])


startval=endval
endval=maxval
for m in range(startval,endval):
    data2=[]
    data2.append([prev_value_discont(data1,m),prev_value_restart(data1,m)])
    data1.extend(data2)

print("combining both")

data=data1

df=pd.DataFrame(data,columns=["discont","restart"])

result=pd.concat([df,spark_df['week_ending']],axis=1,ignore_index=True)

sdf = spark.createDataFrame(result)
sdf.write.mode("overwrite").saveAsTable("")
