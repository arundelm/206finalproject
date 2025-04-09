
def get_api_key(index):
    with open('.env', 'r') as f:
        if index == 1:
            return f.readline().strip()
        elif index == 2:
            f.readline()
            return f.readline().strip()