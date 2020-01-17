import re
def extract_phone(data):
    r = re.compile(r'[-\s]([2-9][0-8][0-9][-\.\s]??[2-9][0-9][0-9][-\.\s]??\d{4}|\([2-9][0-8][0-9]\)\s[2-9][0-9][0-9][-\.\s]??\d{4}|\([2-9][0-8][0-9]\)[-\.\s]??[2-9][0-9][0-9][-\.\s]??\d{4})[\s]')
    phone_numbers = r.findall(data)
    print(phone_numbers)
    a = phone_numbers
   
    if len(a) == 0:
        return "Null"
    elif len(a) == 1:
        primary = a[0]
        
        return primary
    else:
        primary = []
        b = a[0]
        primary.append(b)
        
        secondry = a[1:]
        list(set(secondry))
        secondry = list(set(secondry)-set(primary))
        
        
        final = primary+secondry
        b = ', '.join(final)
        
        return b
