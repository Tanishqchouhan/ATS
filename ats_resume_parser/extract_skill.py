from itertools import chain
import nltk
from nltk.util import ngrams
import db_config
import pymysql
from db_config import *

def extract_skills(data1):
    skills = []
    rows = []
    
    """set connection properties"""
    db_host  = db_config.db_endpoint
    username = db_config.db_username
    pwd = db_config.db_password
    db_name = db_config.db_name
    
    connection = pymysql.connect(host= db_host,user=username,password=pwd,database=db_name)
    
    try:
        
        sql_select_Query = "select skill_name from skill_set"
        cursor = connection.cursor()
        cursor.execute(sql_select_Query)
        records = cursor.fetchall()
        out = [item for t in records for item in t]
        while("" in out):
            out.remove("")
        for every in out:
            rows.append(every.lower())
        data = data1.lower()
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
        
        for every in rows:
            if every in li:
                skills.append(every)
        
     
        final = []
        for eve in skills:
            if eve not in final:
                final.append(eve)
            else:
                pass
            
        b =  ', '.join(final)
        
        
    except Error as e:
        b = 'NULL'
    finally:      
        connection.close()
        cursor.close()

    return b





'''from itertools import chain
import nltk
from nltk.util import ngrams

def extract_skills(data1):

    skills = []
    rows = []
    with open(r'skills.csv', 'r',encoding="ISO-8859-1") as csvfile:
    
        for row in csvfile:
            rows.append(row.replace("\n",'').lower())
    
    
    tokenize = nltk.word_tokenize(data1)
    fourgrams=ngrams(tokenize,4)
    trigrams=ngrams(tokenize,3)
    bigrams=ngrams(tokenize,2)
    
    
    li = []
    generator3 = chain(fourgrams,trigrams,bigrams)
    for item in generator3:
        str = ' '.join(item)
        li.append(str)
        
    for every in rows:
        if every in li:
            skills.append(every)
    
    x = np.array(skills) 
    a = (np.unique(x))
    b = " ,".join(a)
    return b  '''
