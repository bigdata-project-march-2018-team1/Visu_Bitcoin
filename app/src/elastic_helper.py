def http_auth(elastic_conf):
    return "{0}:{1}".format(elastic_conf["username"], elastic_conf["password"])