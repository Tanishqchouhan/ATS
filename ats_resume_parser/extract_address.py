import re
def extract_address(data):
    r = re.compile('\d{1,4} [\w\s]{1,20}[\s](?:street|st|avenue|ave|road|rd|highway|hwy|square|sq|trail|trl|drive|dr|court|ct|parkway|pkwy|circle|cir|boulevard|blvd)\W?(?=\s|$)', re.IGNORECASE)
    address = r.findall(data)
    try:
        return(address[0])
    except:
        return "Null"

