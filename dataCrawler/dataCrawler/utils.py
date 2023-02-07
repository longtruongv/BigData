def parse_num(x):
    if not isinstance(x, str):
        return x
        
    try:
        return int(x)
    except:
        try:
            return float(x)
        except:
            return x

def parse_id_from_uri(uri):
    if not isinstance(uri, str):
        return uri

    if not uri.startswith('/en/'):
        return uri

    uri_list = uri.split('/')
    if len(uri_list) >= 3:
        return uri_list[3]

    return uri