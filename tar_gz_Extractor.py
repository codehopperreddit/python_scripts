#@author: sagnik
# Script to take a .tar.gz file from a S3 location and place the result in another/same bucket . 
import os
import sys
from datetime import datetime
import boto3
import tarfile as tr

#Working directory name here
#os.chdir("")

# create an STS client object that represents a live connection to the 
# STS service
sts_client = boto3.client('sts')

# Call the assume_role method of the STSConnection object and pass the role
# ARN and a role session name.
assumed_role_object=sts_client.assume_role(
    RoleArn="",
    RoleSessionName="Extractor"
)

# From the response that contains the assumed role, get the temporary 
# credentials that can be used to make subsequent API calls
credentials=assumed_role_object['Credentials']

# Use the temporary credentials that AssumeRole returns to make a 
# connection to Amazon S3  
s3=boto3.resource(
    's3',
    aws_access_key_id=credentials['AccessKeyId'],
    aws_secret_access_key=credentials['SecretAccessKey'],
    aws_session_token=credentials['SessionToken'],
)

print("S3 Connection Set Up!")

#bucket and folder in bucket to extract from (source)
bucket = ""
folders3=""

# filename is to be passed in argument
file=sys.argv[1]
#destination bucket and folder
bucket2=""
folders32=""

print("File to be extracted: "+file)

s3.meta.client.download_file(bucket,folders3+file,file)
print("\nFile Download Complete! ")
#tar file open
print("Reading File....")
f=tr.open(file)

files=f.getnames()
print("\nFiles in "+file+":")
for a in files:
	print(a)
print("File Extracting.....")
#extracting files to working directory
f.extractall('./')
print("\nFile Extraction Complete! ")

print("\nUploading Files: ")
#uploading all the files
for a in files:
	s3.meta.client.upload_file(a,bucket2,folders32+a)
	print("Uploaded: "+a)

f.close()

print("\nCleaning up")
for a in files:
	os.system("rm -rf "+a)
	
os.system("rm -rf "+file)
print("\nScript execution ended at "+str(datetime.today().strftime('%Y-%m-%d-%H-%M-%S')))  


exit(0)  
