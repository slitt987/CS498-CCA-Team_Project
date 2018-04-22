from elasticsearch import Elasticsearch, helpers, RequestsHttpConnection
import elasticsearch
import datetime
import json
from .common import *
import re


class IndexData:
    def __init__(self, url, index, doc_type="doc", connection_options={}, index_settings={}, index_mappings=None, alias=False):
        if type(url) is not list:
            url = [url]

        index_mappings = index_mappings if index_mappings is not None else {"properties": {}}

        port = connection_options.pop("port", None)
        full_urls = []
        for addr in url:
            if type(addr) is dict:
                full_urls.append(addr)
            elif ":" in addr:
                full_urls.append({"host": addr.split(":")[0], "port": int(addr.split(":")[1])})
            elif port is not None:
                full_urls.append({"host": addr, "port": int(port)})
            else:
                full_urls.append(addr)

        use_ssl = connection_options.pop("use_ssl", None)
        if use_ssl is not None:
             connection_options["use_ssl"] = bool(use_ssl)

        verify_certs = connection_options.pop("verify_certs", None)
        if verify_certs is not None:
             connection_options["verify_certs"] = bool(verify_certs)

        connection_class = connection_options.pop("connection_class", None)
        if type(connection_class) is type:
             connection_options["connection_class"] = connection_class
        elif connection_class == "RequestsHttpConnection":
             connection_options["connection_class"] = RequestsHttpConnection

        self.__client = Elasticsearch(full_urls, **connection_options)
        alias_exists = self.__client.indices.exists_alias(name=index)
        self.__alias = alias_exists or alias
        self.__doc_type = doc_type
        self.__default = index_mappings.pop("_default_", {})
        self.__index_settings = index_settings
        self.__index_mappings = index_mappings

        self.__index_settings["number_of_shards"] = int(self.__index_settings.get("number_of_shards", 1))
        self.__index_settings["number_of_replicas"] = int(self.__index_settings.get("number_of_replicas", 0))

        if alias_exists:
            self.created = False
            self.__alias_name = index
            self.__index = self.get_alias_index()
        elif alias:
            self.__alias_name = index
            self.__index = "{}-1".format(index)
            self.create_if_not_exists()
            self.update_alias()
        else:
            self.__index = index
            self.create_if_not_exists()

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

    def get_result_source(self, results):
        return results.get("hits").get("total"), \
               [byteify(data.get("_source")) for data in results.get("hits").get("hits")]

    def scan(self, scroll_ttl='5m', query=None, ids=False):
        if query is None:
            query = {
                "query": {
                    "match_all": {}
                }
            }

        results = helpers.scan(self.__client, index=self.__index, doc_type=self.__doc_type, query=query, scroll=scroll_ttl)
        if ids:
            return ((byteify(result.get("_id")), byteify(result.get("_source"))) for result in results)
        else:
            return (byteify(result.get("_source")) for result in results)

    def load(self, ids=False):
        if ids:
            return dict(self.scan(ids=True))
        else:
            return list(self.scan())

    def search_terms(self, terms):
        term_list = []
        for term in terms:
            value = terms.get(term)
            if type(value) is list:
                op = "terms"
            else:
                op = "term"

            term_list.append({op: {term: value}})
        query = {"query": {"constant_score": {"filter": {"bool": {"must": term_list}}}}}
        # print "DEBUG QUERY: {}".format(json.dumps(query, indent=4, sort_keys=True))
        return self.scan(query=query)

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
                self.create_if_not_exists()

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

    def is_alias(self):
        return self.__alias

    def create_if_not_exists(self, index=None):
        index = index if index is not None else self.__index
        if not self.__client.indices.exists(index):
            self.created = True
            self.__client.indices.create(index=index,
                                         body={"mappings": {self.__doc_type: self.__index_mappings, "_default_": self.__default},
                                               "settings": self.__index_settings})
        else:
            self.created = False

    def write(self, data, id=None):
        for key in data:
            if isinstance(data.get(key), datetime.datetime):
                data[key] = data.pop(key).strftime('%Y-%m-%dT%H:%M:%S%z')
        try:
            request = {"index": self.__index, "doc_type": self.__doc_type, "body": data}
            if id is not None:
                request["id"] = id

            self.__client.index(**request)
        except elasticsearch.exceptions.RequestError as e:
            eprint("bad doc: {}".format(data))
            eprint(e)
            exit(1)

    def dump(self, data):
        if type(data) is list:
            for row in data:
                self.write(row)
        if type(data) is dict:
            for id in data:
                self.write(data.get(id), id=id)
        else:
            self.write(data)
