def read_config(filename):
    import json
    config = json.load(open(filename))
    return config