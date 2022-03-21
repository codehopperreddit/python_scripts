# Output Script
#Takes output from a table and generates a excel document and then places it on S3
#Sagnik Chatterjee . 
import os
import sys
import pandas as pd
from datetime import datetime
import boto3
os.environ['SPARK_HOME'] = "/usr/lib/spark/"
sys.path.append("/usr/lib/spark/python/")
from pyspark.sql import SparkSession


s3_client = boto3.client('s3',aws_access_key_id="",aws_secret_access_key="")
bucket = ""

spark = SparkSession.builder.enableHiveSupport().appName("Summary document").getOrCreate()

folder = ""

#Raw file creation

file_name=" file "+str(datetime.today().strftime('%Y-%m-%d___%H-%M-%S'))+".xlsx"



query = ""
df = spark.sql(query)


#Raw file Query

query=[""]

sheetnm=['','','','']
writer = pd.ExcelWriter(file_name,engine='xlsxwriter')
for i in range(0,4):
    df = spark.sql(query[i])
    data=df.toPandas()
    data.to_excel(writer,sheet_name=sheetnm[i],startrow=1,header=True,index=False)
   
# Mail Text Query

query=[""] 

numdf=["","","","","","","","","","","","","","","",""]

for i in range(0,16):
    num = spark.sql(query[i])
    numtr=num.toPandas()
    numdf[i]=numtr.ix[0,0]
  

 
    
df = pd.DataFrame({'': ['','' ,'' ,'','','','','','','','','','','','',''],'Count' :[numdf[0],numdf[1],numdf[2],numdf[3],numdf[4],numdf[5],numdf[6],numdf[7],numdf[8],numdf[9],numdf[10],numdf[11],numdf[12],numdf[13],numdf[14],numdf[15]]})

df= df [['','']]

df.to_excel(writer,sheet_name='Mail',startrow=1,header=False,index=False)


workbook = writer.book
worksheet = writer.sheets['Mail']

(max_row, max_col) = df.shape

column_settings = [{'header': column} for column in df.columns]

worksheet.add_table(0, 0, max_row, max_col - 1, {'columns': column_settings})

worksheet.set_column(0, max_col - 1, 12)




# save the file
workbook.set_properties({'author':'Sagnik Chatterjee'})
writer.save() 
print("\nScript execution ended at "+str(datetime.today().strftime('%Y-%m-%d___%H-%M-%S')))  

s3_client.upload_file(file_name,bucket,folder+file_name)
os.system("rm -rf *.xlsx")


exit(0)  