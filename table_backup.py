# script to table table backup on hive
import os
import sys
import datetime
import smtplib

os.environ['SPARK_HOME'] = "/usr/lib/spark/"
sys.path.append("/usr/lib/spark/python/")
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark import SparkContext
hive_database = ""

spark = SparkSession.builder.enableHiveSupport().appName("backup").getOrCreate()

date_now = datetime.datetime.now()
current_date = date_now.strftime("%Y%m%d")

def fullbck():
    backup_table = ["", ""]
    for table in backup_table:
        print("Dropping table if present: {1}_BKP_{2} ".format(hive_database,table,current_date))
        query = ''' drop table if exists {0}.{1}_BKP_{2} '''.format(hive_database,table,current_date)
        spark.sql(query)
        print("Backing up table as: {1}_BKP_{2}".format(hive_database,table,current_date))
        query = ''' CREATE TABLE {0}.{1}_BKP_{2} AS SELECT * FROM {0}.{1} '''.format(hive_database, table,current_date)
        spark.sql(query)


    print("Table List for refernce:")

    for table in backup_table:
        print("{1}_BKP_{2}".format(hive_database,table,current_date))
        

def rpt_bck():
    backup_table = [""]

    for table in backup_table:
        print("Dropping table if present: {1}_BKP_{2} ".format(hive_database,table,current_date))
        query = ''' drop table if exists {0}.{1}_BKP_{2} '''.format(hive_database,table,current_date)
        spark.sql(query)
        print("Backing up table as: {1}_BKP_{2}".format(hive_database,table,current_date))
        query = ''' CREATE TABLE {0}.{1}_BKP_{2} AS SELECT * FROM {0}.{1} '''.format(hive_database, table,current_date)
        spark.sql(query)


    print("Table List for refernce:")

    for table in backup_table:
        print("{1}_BKP_{2}".format(hive_database,table,current_date))
        
if __name__ == '__main__':


        if sys.argv[1]=="0":
                fullbck()
        elif sys.argv[1]=="1":
                rpt_bck()
        else:
                print ("\n\nNo Valid Arguement Passed")     
        
   
    
print("Script completed ")

exit(0)  