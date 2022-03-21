# Script to take columns from a hive table and outputs to a excel file , then formats it and uploads to box and a S3 location
import os
os.system("sudo pip install xlsxwriter")
os.system("sudo pip install boxsdk[jwt]")
import sys
import pandas as pd
from datetime import datetime
from datetime import date, timedelta 
from dateutil.relativedelta import relativedelta, FR
import xlsxwriter
import boto3
os.environ['SPARK_HOME'] = "/usr/lib/spark/"
sys.path.append("/usr/lib/spark/python/")
from pyspark.sql import SparkSession
from boxsdk import JWTAuth, Client


s3_client = boto3.client('s3',aws_access_key_id="",aws_secret_access_key="")
bucket = ""

def get_col_widths(dataframe):
    # First we find the maximum length of the index column   
    idx_max = max([len(str(s)) for s in dataframe.index.values] + [len(str(dataframe.index.name))])
    # Then, we concatenate this to the max of the lengths of column name and its values for each column, left to right
    return [idx_max] + [max([len(str(s)) for s in dataframe[col].values] + [len(col)]) for col in dataframe.columns]


spark = SparkSession.builder.enableHiveSupport().appName("name").getOrCreate()

folder = ""
file_name=str(datetime.today().strftime('%Y.%m.%d'))+"File Name - Data as of "+str(((datetime.today()- timedelta(days=1))+relativedelta(weekday=FR(-1))).strftime('%m.%d'))+".xlsx"
# date for adding in text
fridate=str((((datetime.today()- timedelta(days=1))- timedelta(days=1))+relativedelta(weekday=FR(-1))).strftime('%m/%d'))

query=[""]

top_info = pd.DataFrame({'Data': ['info '+fridate ,'info' ,'' ,'',' ','','']})

notes = pd.DataFrame({'*note': ['*note']})

sheetnm=['Sheet 1','Sheet 2'] 
writer = pd.ExcelWriter(file_name,engine='xlsxwriter',options={'strings_to_numbers': True})
for i in range(0,2):
    df = spark.sql(query[i])
    data=df.toPandas()
    data.to_excel(writer,sheet_name=sheetnm[i],startrow=10,startcol=1,header=False,index=False)
    top_info.to_excel(writer,sheet_name=sheetnm[i],startrow=0,startcol=1,index=False)
    notes.to_excel(writer,sheet_name=sheetnm[i],startrow=8,startcol=9,header=False,index=False)
    notes.to_excel(writer,sheet_name=sheetnm[i],startrow=8,startcol=68,header=False,index=False)
    dftotal=spark.sql(query[i+2])
    total=dftotal.toPandas()
    total.to_excel(writer,sheet_name=sheetnm[i],startrow=9,startcol=1,header=False,index=False)
    workbook  = writer.book
    worksheet = writer.sheets[sheetnm[i]]
    header_format = workbook.add_format({
    'bold': True,
    'text_wrap': True,
    'valign': 'vcenter',
    'align' : 'center',
    'fg_color': '#E4DFEC',
    'border': 1})
    
    for col_num, value in enumerate(data.columns.values):
        worksheet.write(10, col_num + 1, value, header_format)
    border_fmt = workbook.add_format({'bottom':1, 'top':1, 'left':1, 'right':1})
    worksheet.conditional_format(xlsxwriter.utility.xl_range(10, 1, len(data)+9, len(data.columns)), {'type': 'no_errors', 'format': border_fmt})
    num_fmt1=workbook.add_format({'num_format': '0'})
    worksheet.conditional_format(xlsxwriter.utility.xl_range(11, 4, len(data)+9, len(data.columns)), {'type': 'no_errors', 'format': num_fmt1})
    num_fmt2=workbook.add_format({'num_format': '0.0'})
    worksheet.conditional_format(xlsxwriter.utility.xl_range(11, 2, len(data)+9, 4), {'type': 'no_errors', 'format': num_fmt2})
    worksheet.conditional_format(xlsxwriter.utility.xl_range(9, 2, len(total),len(total.columns) ), {'type': 'no_errors', 'format': num_fmt2})
    center_align=workbook.add_format()
    center_align.set_align('center')
    for i in range(9,len(data)+12):
        worksheet.set_row(i, None, center_align)
    center_align.set_align('vcenter')
    for i in range(9,len(data)+12):
        worksheet.set_row(i, None, center_align)
    worksheet.hide_gridlines(2)
    worksheet.hide_row_col_headers()
    worksheet.freeze_panes(11, 2)    
    worksheet.autofilter('B11:CJ11')
    for i, width in enumerate(get_col_widths(data)):
        worksheet.set_column(i, i, width)
writer.save()        

s3_client.upload_file(file_name,bucket,folder+file_name)

print("\n Uploaded to S3 "+str(datetime.today().strftime('%Y-%m-%d-%H-%M-%S')))  

print("Setting up connection to Box : ")
config = JWTAuth.from_settings_file('revo_private.json')
client = Client(config)
client.user().get()

#box folder id
folder_id = ''

new_file = client.folder(folder_id).upload(''+file_name)
print('File "{0}" uploaded to Box with file ID {1}'.format(new_file.name, new_file.id))


os.system("rm -rf *.xlsx")


print("\n ScripT end "+str(datetime.today().strftime('%Y-%m-%d-%H-%M-%S')))  

exit(0)  