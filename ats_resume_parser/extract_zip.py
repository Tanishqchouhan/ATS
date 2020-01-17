import re
from db_config import *
import pymysql

def extract_zip(data,city):
    # check for empty zip is remaining here 
    r = re.compile(r'[0-9]{5}')
    zip_code_list = r.findall(data)
    try:
        zip_code = zip_code_list[0]
        
        """set connection properties"""
         
        if city==None:
            #if city is None , find the city against the zip code we have
            db_host  = db_endpoint
            username = db_username
            pwd = db_password
            database_name = db_name
            
            connection = pymysql.connect(host= db_host,user=username,password=pwd,database=database_name)            
            cursor = connection.cursor()
            cursor.execute("select city from city_zip_map where zip_code=%s",(zip_code,))          
            print("The query affected {} rows".format(cursor.rowcount))
            count = cursor.rowcount
            print(count)
            
            if count is 0:
                print('passed zip code does not exists in db')
                zip_city_dict = {'zip':zip_code,'city':"Null"}
                return zip_city_dict
            else:
                result_set  = cursor.fetchall()                                       
                for row in result_set:
                    extracted_city = row[0]
                    
                print('extracted_city............',extracted_city)              
                print('city mapped with zip_code in db............',extracted_city)            
                zip_city_dict = {'zip':zip_code,'city':extracted_city}  
                print(zip_city_dict)
                return zip_city_dict
            
        else:
            print('City has been extracted already')  
            zip_city_dict = {'zip':zip_code,'city':city}
            return zip_city_dict
       
    except:
        print('city not extracted from db')  
        return "Null"
