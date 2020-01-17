import re
from nltk.tokenize import word_tokenize 
import os
import pymysql
from boto3 import client
from urllib.parse import unquote_plus
import boto3
from db_config import *
#parser files
from ats_resume_parser.extract_cities import *
from ats_resume_parser.extract_mail import *
from ats_resume_parser.extract_phone import *
from ats_resume_parser.extract_address import *
from ats_resume_parser.extract_zip import *
from ats_resume_parser.extract_name import *
from ats_resume_parser.extract_skill import *
from ats_resume_parser.extract_state import *
#publisher lib
import pika
import json

def get_raw_content(localfilename):
    print('Getting raw content')        
    f1 =  open(localfilename,encoding = 'utf8',errors = 'ignore')
    text = (f1.read())
    text = re.sub(r'[^\x00-\x7F]+',' ', text)
    a = text.replace('\n', ' ')
    a = a.replace('\t', ' ')
    data =  re.search('%s(.*)%s' % ('<pre>', '</pre>'),a).group(1)
    author = re.search('%s(.*)%s' % ('<title>', '</title>'),a).group(1)
    author= author.replace('Author:','')
    data = data.replace(author,'',1)
    return data
    
def delete_files(intermediate_object_key,source_object_key,i_bucket,s_bucket):
    print("delete file function worked")
    #delete from parser_bucket after getting null in email
    client('s3').delete_object(Bucket=i_bucket, Key=intermediate_object_key)
    #delete from source bucket as well
    client('s3').delete_object(Bucket=s_bucket, Key=source_object_key)

    
def lambda_handler(event, context):
    s3 = boto3.client('s3')
    
    try:
        #extract record from the event occurred
        for record in event['Records']:
            bucket = record['s3']['bucket']['name']
            key = unquote_plus(record['s3']['object']['key']) 
            localFilename = '/tmp/{}'.format(os.path.basename(key))

            s3.download_file(Bucket=bucket, Key=key, Filename=localFilename)
            inFile = open(localFilename, "r")
            
            #extract raw content from text file
            if localFilename.endswith('.txt'):
                data = get_raw_content(localFilename)
            else:
                data = (localFilename + "file type not compatible")

            """set connection properties"""
            db_host  = db_endpoint
            username = db_username
            pwd = db_password
            database_name = db_name
                           
            db = pymysql.connect(host= db_host,user=username,password=pwd,database=database_name)
            cursor = db.cursor()
            s3_resource = boto3.resource('s3')
            source_bucket =   dev_source_bucket  
            destination_bucket = dev_destination_bucket
            intermediate_bucket = dev_intermediate_bucket
            
        try:

            mail = extract_mail(data) 
            extention_list = key.split(".", -2)
            # extention_list[-1] == txt
            # extention_list[-2] == rtf,doc,docx,pdf
           
            if (not mail.strip() or mail=="Null"):
                delete_files(key,key.replace("."+extention_list[-1], ""),intermediate_bucket,source_bucket)
                
            else:
                name = extract_name(data)
                city = extract_cities(data)
                
                #check if the city was found 
                #if city is not found, grab the zip code and find the city against that zip_code from db
                
                if (city=="" or city=="NULL" or city==None):
                    extract_zip_code = extract_zip(data,None)  
                    zip_code =   extract_zip_code['zip']  
                    city = extract_zip_code['city']
                    if city==None:
                        print('passed zip code does not exists in db')
                else:
                    extract_zip_code = extract_zip(data,city)
                    zip_code =   extract_zip_code['zip']  
                    city = extract_zip_code['city']
                    
                address = extract_address(data)
                phone = extract_phone(data)  
                state = extract_state(data)            
                skill = extract_skills(data)
                
                print('name:'+name)
                print('address:'+address)
                print('zip_code:'+zip_code)
                print('phone:'+phone)
                print('state:'+state)
                print('city:'+city)
                print('mail:'+mail)
                
                #alternate phone number check
                if "," in phone:
                    phone_number_list = phone.split(",", 1)        
                    primary_phone =  phone_number_list[0]
                    secondary_phone = phone_number_list[1]
                else:
                    primary_phone =  phone
                    secondary_phone = None
         
                if "," in mail:
                    email_list = mail.split(",", 1)
                    primary_mail = email_list[0]
                    additional_mail = email_list[1]                   
                    text_filename = email_list[0].replace('@','~') + "." + extention_list[-1]
                    destination_filename = email_list[0].replace('@','~') + "." + extention_list[-2]  	   
                else:
                    primary_mail = mail
                    additional_mail = None
                    text_filename = mail.replace('@','~') + "." + extention_list[-1]
                    destination_filename = mail.replace('@','~') + "." +  extention_list[-2]
                       
                    
                try:
                    cursor.execute("INSERT INTO resumes(resume_path,name,mobile,location,email,city,state,zipcode,skills,resumes_values,additional_email,alternate_contact_number)VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",(destination_filename,name,primary_phone,address,primary_mail,city,state,zip_code,skill,data,additional_mail,secondary_phone,))
                    print("New record updated successfully! ")
                    db.commit() 
                except pymysql.IntegrityError as err:
                    cursor.execute("Update resumes set resume_path = %s,name = %s,mobile = %s,location = %s,city = %s,state = %s,zipcode = %s,skills = %s ,resumes_values = %s,alternate_contact_number =  %s,additional_email = %s where email = %s",(destination_filename,name,primary_phone,address,city,state,zip_code,skill,data,secondary_phone,additional_mail,primary_mail,))
                    db.commit() 
                    print("This resume has been already parsed and now updated.")
                  
                #copy file from s1 to s3 (one with actual file format,renamed with email-id) and give it public access
                copy_source = {'Bucket': source_bucket,'Key': key.replace("."+extention_list[-1],"")}  
                extra_args = {
                    'ACL': 'public-read'
                }
                s3_resource.meta.client.copy(copy_source,destination_bucket,destination_filename,extra_args)  
                
              
                #copy file from s2 to s3 (one with text file format,renamed with email-id) and give it public access
                copy_source_textfile = {'Bucket': intermediate_bucket,'Key': key}  
                s3_resource.meta.client.copy(copy_source_textfile,destination_bucket,text_filename)
                
                
                #give file in s3 a public read so that it can be pulled from ec2
                object_acl = s3_resource.ObjectAcl(destination_bucket,text_filename)
                response = object_acl.put(ACL='public-read')
                print(response)
                
                
                print("publisher code begin")
                url = 'amqp://vtsystem:vtsystem@13.57.141.91/%2f'
                params = pika.URLParameters(url)
                params.socket_timeout = 5
                connection = pika.BlockingConnection(params) # Connect to CloudAMQP
                channel = connection.channel() # start a channel
                channel.queue_declare(queue='pdfprocess') # Declare a queue
                thisdict = {
                            'url': 'https://s3-us-west-1.amazonaws.com/dev.target.ats/'+text_filename,
                            'filename': text_filename
                            }
                channel.basic_publish(exchange='', routing_key='pdfprocess', body=json.dumps(thisdict))
                print ("Message sent to consumer")
                connection.close()
                print("publisher code end")
  
                delete_files(key,key.replace("."+extention_list[-1], ""),intermediate_bucket,source_bucket)
                           
        except Exception as e:
            print(e)
            delete_files(key,key.replace("."+extention_list[-1], ""),intermediate_bucket,source_bucket)
            
    
    except Exception as e:
        print(e) 
            
            
       
        
