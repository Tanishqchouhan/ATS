from itertools import chain
from db_config import *
from nltk.tokenize import word_tokenize 
import pymysql
def extract_state(data):

    """set connection properties"""
    db_host  = db_endpoint
    username = db_username
    pwd = db_password
    database_name = db_name
    
    connection = pymysql.connect(host= db_host,user=username,password=pwd,database=database_name)
    

    sql_select_Query = "select state,state_id from state"
    cursor = connection.cursor()
    cursor.execute(sql_select_Query)
    records = cursor.fetchall()
    state_list = list(chain.from_iterable(records))
    
    #lowercase the state list
    state_list_lower = []
    for state in state_list:
        state_list_lower.append(state.lower())
       
                   
    word_tokens = word_tokenize(data)
    raw_state = []
    count = 0
    word_token = word_tokens[:200]
    
    states = []
    for state in word_token:
        if state.lower() in state_list_lower:
            states.append(state.lower())
            
    states = list(dict.fromkeys(states)) 
    
    print('all states......',states)
    print('return states which was grabbed first....',states[0]) #return city which was grabbed first
    #city =  ', '.join(cities) 
    return states[0]
    
    
   
    
