import re
def extract_name(data):
    data =  data[0:100].lower()
    try:
        if 'Name' in data:
            splt = data.split()
            list(splt)
            name  = (splt[splt.index('Name') + 1])
            return name

        elif 'NAME' in data:
            splt = data.split()
            list(splt)
            return (splt[splt.index('NAME') + 1])

        else :
            word_tokens =(re.sub(r'[^a-zA-Z ]', '', data).split())
            namelist= []
            co = ["'",",",":","/","(",")",'employee','bechtel','personal','job','placement','office','services','january','february','march','april','may','june','july','august','september','october','november','december','dr','er','prof','curriculum','vitae','director','mktg','communications','marketing','technology','phone','please','docx','doc','email','gmail','yahoo','com','type','texttype','text','references','top','form','professional','summary','work','history','for','college','graduate','recent' ,'indeed','confidential','a','ae','b',"c","d","e","f",'g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z','few','points','to','note','microsoft','word','work','experience','qualification','highlights','page','excel','template','objective','and','resume','wizard','of','hi','name','contact','info']
            for every in word_tokens:
                if every in  co:
                    pass
                else:
                    namelist.append(every)
               
            a = namelist [:2]
            s = ' '
            
            name = (s.join(a))
            list = ['resume','docx','doc','email','gmail','yahoo','com']
            for i in list:
                a = name.replace(i, '')
                name = a
            return a
    except:
        return ''
    
    


    

