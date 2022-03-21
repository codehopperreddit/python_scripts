import os
import sys
import pandas as pd
import re


os.environ['SPARK_HOME'] = "/usr/lib/spark/"
sys.path.append("/usr/lib/spark/python/")
from pyspark.sql.types import *
from pyspark.sql import SparkSession

spark = SparkSession.builder.enableHiveSupport().appName("standadizing  names").getOrCreate()
spark.sql("")

std_table= spark.sql("select * from ").toPandas()
unmapped= spark.sql("select * from ").toPandas()


for ind in std_table.index:
    find=std_table['src_val'][ind].encode('utf-8').strip()
    replace=std_table['tgt_val'][ind].encode('utf-8').strip()
    for jnd in unmapped.index
    #add  column name
        row=unmapped['column'][jnd].encode('utf-8').strip()
        row=re.sub(r"\b%s\b" % find.decode('utf-8').strip(),replace.decode('utf-8').strip(),row.decode('utf-8').strip()).decode('utf-8').strip() # \b is the word boundary
        print(row)
        unmapped['column'][jnd]=row
    print('Replacing :'+std_table['src_val'][ind]+" with " + std_table['tgt_val'][ind])

df_2=unmapped.astype(str)
sdf_2 = spark.createDataFrame(df_2)
sdf_2.write.mode("overwrite").saveAsTable("")

