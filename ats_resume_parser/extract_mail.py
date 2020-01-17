import re
def extract_mail(data):
    r = re.compile(r'[\w\.-]+@[\w]+.{2,3}[a-zA-Z]')
    data = data.lower()
    
    a = r.findall(data)
    
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
