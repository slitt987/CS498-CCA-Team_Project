from elasticsearch import Elasticsearch
import elasticsearch
import types
import datetime
from .common import *


class ElasticWriter:
    def __init__(self, url, index, doc_type="doc", connection_options={}, index_settings={}, index_mappings={"properties":{}}):
        if not isinstance(url, types.ListType):
            url = [url]

        self.__client = Elasticsearch(url, **connection_options)
        self.__index = index
        self.__doc_type = doc_type
        if not self.__client.indices.exists(self.__index):
            index_settings["number_of_shards"] = int(index_settings.get("number_of_shards", 1))
            index_settings["number_of_replicas"] = int(index_settings.get("number_of_replicas", 0))
            self.__client.indices.create(index=self.__index, body={"mappings": {doc_type: index_mappings}, "settings": index_settings})

    def write(self, data):
        for key in data:
            if isinstance(data.get(key), datetime.datetime):
                data[key] = data.pop(key).strftime('%Y-%m-%dT%H:%M:%S%z')
        try:
            self.__client.index(index=self.__index, doc_type=self.__doc_type, body=data)
        except elasticsearch.exceptions.RequestError as e:
            eprint("bad doc: {}".format(data))
            eprint(e)
            exit(1)
