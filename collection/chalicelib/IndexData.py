from elasticsearch import Elasticsearch
import elasticsearch
import types
import datetime
from .common import *
import re


class IndexData:
    def __init__(self, url, index, doc_type="doc", connection_options={}, index_settings={}, index_mappings={"properties":{}}, alias=False):
        if not isinstance(url, types.ListType):
            url = [url]

        self.__client = Elasticsearch(url, **connection_options)
        alias_exists = self.__client.indices.exists_alias(name=index)
        self.__alias = alias_exists or alias
        self.__doc_type = doc_type
        self.__index_settings = index_settings
        self.__index_mappings = index_mappings

        self.__index_settings["number_of_shards"] = int(self.__index_settings.get("number_of_shards", 1))
        self.__index_settings["number_of_replicas"] = int(self.__index_settings.get("number_of_replicas", 0))

        if alias_exists:
            self.__alias_name = index
            self.__index = self.get_alias_index()
        elif alias:
            self.__alias_name = index
            self.__index = "{}-1".format(index)
            self.create_if_not_exists(index)
            self.update_alias()
        else:
            self.__index = index
            self.create_if_not_exists(index)

        self.creation_date = self.get_index_creation_date()

    def get_index_creation_date(self, index=None, settings=None):
        if index is None:
            index = self.__index

        if settings is None:
            settings = byteify(self.__client.indices.get_settings(index=index)).get(index)

        return from_epoch(float(settings.get("settings").get("index").get("creation_date")) / 1000)

    def get_alias_index(self):
        self.check_is_alias()
        alias_data = byteify(self.__client.indices.get_alias(name=self.__alias_name))
        return alias_data.keys()[0]

    def purge_alias_index(self, ttl=86400):
        self.check_is_alias()
        alias_index = self.get_alias_index()
        indexes = self.__client.indices.get("{}-*".format(self.__alias_name))
        for index in indexes:
            if index == self.__index or index == alias_index:
                continue

            settings = byteify(self.__client.indices.get_settings(index=index)).get(index)
            creation_date = self.get_index_creation_date(settings=settings)
            index_age = to_epoch(datetime.datetime.now()) - to_epoch(creation_date)
            if index_age > ttl:
                self.__client.indices.delete(index)

    def get_index(self):
        return self.__index

    def get_client(self):
        return self.__client

    def get_next_alias_index(self):
        self.check_is_alias()

        index = None
        if re.match("^{}-\\d+$".format(self.__alias_name), self.__index) is not None:
            i = int(re.sub("^{}-".format(self.__alias_name), "", self.__index))
        else:
            i = 0

        while index is None:
            i = i + 1
            index = "{}-{}".format(self.__alias_name, i)
            if self.__client.indices.exists(index):
                index = None
            else:
                self.__index = index
                self.create_if_not_exists(index)

        return index

    def update_alias(self):
        self.check_is_alias()

        if self.__client.indices.exists_alias(name=self.__alias_name):
            curr_index = self.get_alias_index()
            if curr_index != self.__index:
                self.__client.indices.update_aliases(
                    {"actions": [
                        {"remove": {"index": curr_index, "alias": self.__alias_name}},
                        {"add": {"index": self.__index, "alias": self.__alias_name}}
                    ]})

        self.__client.indices.put_alias(self.__index, self.__alias_name)

    def check_is_alias(self):
        if not self.__alias:
            raise ValueError("Index requested not an alias: {}".format(self.__index))

    def create_if_not_exists(self, index):
        if not self.__client.indices.exists(index):
            self.__client.indices.create(index=index,
                                         body={"mappings": {self.__doc_type: self.__index_mappings},
                                               "settings": self.__index_settings})

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
