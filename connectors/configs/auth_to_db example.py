class ConnectCH:
    hostname = ""
    database = ""
    username = ""
    pwd = ""
    port_id = 8123

    url = f"clickhousedb+connect://{username}:{pwd}@{hostname}:{port_id}/{database}?compression=zstd"