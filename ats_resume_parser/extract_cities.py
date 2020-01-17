
from itertools import chain
import nltk
from nltk.util import ngrams
import pymysql
from db_config import *

def extract(data,rows):
    data = ''.join(data)
    tokenize = nltk.word_tokenize(data)
    fourgrams=ngrams(tokenize,4)
    trigrams=ngrams(tokenize,3)
    bigrams=ngrams(tokenize,2)
    monograms = ngrams(tokenize,1)
    
    li = []
    generator3 = chain(fourgrams,trigrams,bigrams,monograms)
    for item in generator3:
        str1 = ' '.join(item)
        li.append(str1)
      
    cities = []
    for every in li:
        if every in rows:
            cities.append(every)
            
    cities = list(dict.fromkeys(cities)) 
    print('all cities......',cities)
    print('return city which was grabbed first....',cities[0]) #return city which was grabbed first
    #city =  ', '.join(cities) 
    return cities[0]


def extract_cities(input_file_data):
    cities = []
    rows = []
    
    """set connection properties"""
    db_host  = db_endpoint
    username = db_username
    pwd = db_password
    database_name = db_name
    
    connection = pymysql.connect(host= db_host,user=username,password=pwd,database=database_name)
    
    try:
        
        sql_select_Query = "select distinct City from city_state_map where ranking=1"
        cursor = connection.cursor()
        cursor.execute(sql_select_Query)
        records = cursor.fetchall()
        out = [item for t in records for item in t]
        while("" in out):
            out.remove("")
        for every in out:
            rows.append(every.lower())
        data = input_file_data[0:200].lower()

        city  = extract(data,rows)
        if city == '':
            end_file_data = input_file_data[-200:].lower()
            city = extract(end_file_data,rows)
            

    except Exception as e:
        print(e)
        city = 'NULL'
    finally:
        connection.close()
        cursor.close()

    return city

    


    
        



