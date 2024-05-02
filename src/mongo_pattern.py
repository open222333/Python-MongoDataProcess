import re


class MongoUriPattern():

    """
        匹配相關
    """

    uri_pattern = re.compile(r'mongodb://(\w+:\d+/|\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}:\d+)/')
    ip_pattern = re.compile(r'\w+:\d')
    host_pattern = re.compile(r'\d.\d.\d.\d:\d')
