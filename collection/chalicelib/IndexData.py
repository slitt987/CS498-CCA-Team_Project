from elasticsearch import Elasticsearch, helpers, RequestsHttpConnection, NotFoundError
import elasticsearch
import datetime
import json
from .common import *
import re


class IndexData:
    def __init__(self, url, index, doc_type="doc", connection_options={}, index_settings={}, index_mappings=None, alias=False):
        """
        Constructor
        :param url: ES URL (or list of URL's)
        :param index: Name of the index to present
        :param doc_type: document type name to present from the index (Default: doc)
        :param connection_options: map of connection options to pass to the ES client constructor (Default: Empty)
        :param index_settings: storage settings for the index to use on creation (if needed) (Default: Empty)
        :param index_mappings: field mappings to provide for index creation (if needed) (i.e. type control) (Default: None)
        :param alias: is the provided "index" name really an alias (or should it be - on creation)
        """

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
        self.__numeric_fields = None

    def get_index_creation_date(self, index=None, settings=None):
        """
        Retrieves an indexes creation timestamp
        :param index: index name to lookup (Default: self)
        :param settings: object previously fetched from ES that contains creation information (Default: None)
        :return: creation datetime
        """

        if index is None:
            index = self.__index

        if settings is None:
            settings = byteify(self.__client.indices.get_settings(index=index)).get(index)

        return from_epoch(float(settings.get("settings").get("index").get("creation_date")) / 1000)

    def get_alias_index(self):
        """get the first index behind an alias (used for A/B replacement index usage)"""
        self.check_is_alias()
        alias_data = byteify(self.__client.indices.get_alias(name=self.__alias_name))
        return alias_data.keys()[0]

    def scan(self, scroll_ttl='5m', query=None, ids=False):
        """
        Read data from the index into a generator
        :param scroll_ttl: ttl for the scroll (must fetch more results before this time expires - set longer for a slow consumer)
        :param query: ES query to submit (Default: match_all)
        :param ids: should the ES document ids be in the result set
        (if True this returns pairs of (id, source) in the result) (Default: False)
        :return: generator of data
        """
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
        """
        load data to memory from an elastic search index
        :param ids: should the result be a dictionary keyed on ES doc id's or a list of source (Default: false)
        :return: a list or dict of source
        """
        if ids:
            return dict(self.scan(ids=True))
        else:
            return list(self.scan())

    def search_terms(self, terms, numeric_as_min=False):
        """
        searches an ES index for a series of terms
        :param terms: a dictionary of attributes to search with either a single or list of terms to match
        :param numeric_as_min: should numbers be treated as min values (or absolute) (Default: False)
        :return: generator of data
        """

        # Get a list of numeric fields if numbers_as_min set and I don't already have this cached
        if numeric_as_min and self.__numeric_fields is None:
            field_mapping = byteify(
                self.__client.indices.get_mapping(index=self.__index, doc_type=self.__doc_type))
            field_mapping = field_mapping.get(field_mapping.keys()[0])\
                .get("mappings")\
                .get(self.__doc_type)\
                .get("properties")
            self.__numeric_fields = [field for field in field_mapping if
                              field_mapping.get(field).get("type") in ["float", "long", "int"]]

        term_list = []
        for term in terms:
            value = terms.get(term)
            if numeric_as_min and term in self.__numeric_fields:
                if type(value) is list:
                    value = min(value)

                term_list.append({"range": {term: {"gte": value}}})
            else:
                if type(value) is list:
                    op = "terms"
                else:
                    op = "term"

                term_list.append({op: {term: value}})

        query = {"query": {"constant_score": {"filter": {"bool": {"must": term_list}}}}}
        # print "DEBUG QUERY: {}".format(json.dumps(query, indent=4, sort_keys=True))
        return self.scan(query=query)

    def get_doc(self, id, default=None):
        """
        Gets a single document
        :param id: ES Document id
        :return: ES Source
        """
        try:
            return byteify(self.__client.get(self.__index, self.__doc_type, id).get("_source"))
        except NotFoundError:
            return default

    def purge_alias_index(self, ttl=86400):
        """
        purges old indexes that used to be tied to an alias (used for A/B replacement index usage)
        :param ttl: how long to wait before removing an index that used to be tied to the alias (Default: 86400)
        :return: None
        """
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
        """Returns the index name"""
        return self.__index

    def get_client(self):
        """returns the ES client"""
        return self.__client

    def get_next_alias_index(self):
        """Gets the next index name for a given alias (used for A/B replacement index usage)"""
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
        """
        Updates the alias to point to the current index
        (to be used after populating a new index created by get_next_alias_index)
        (used for A/B replacement index usage)
        """
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
        """Ensure that the alias provided really is an alias, not an index (or raise Exception)"""
        if not self.__alias:
            raise ValueError("Index requested not an alias: {}".format(self.__index))

    def is_alias(self):
        """
        Check if the current object is configured as an alias
        :return: boolean
        """
        return self.__alias

    def create_if_not_exists(self, index=None):
        """
        Creates an index if it doesn't already exist in ES
        :param index: index name to create
        :return: None
        """
        index = index if index is not None else self.__index
        if not self.__client.indices.exists(index):
            self.created = True
            self.__client.indices.create(index=index,
                                         body={"mappings": {self.__doc_type: self.__index_mappings, "_default_": self.__default},
                                               "settings": self.__index_settings})
        else:
            self.created = False

    def write(self, data, id=None):
        """
        Writes data into the index
        :param data: source data to write into the index
        :param id: document id to write to (Default: None - auto id)
        :return: None
        """
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
        """
        Unpacks a data iterable and writes data to ES
        :param data: data to write to the index (dictionary will use the dictionary key as the doc id)
        :return: None
        """
        if type(data) is list:
            for row in data:
                self.write(row)
        if type(data) is dict:
            for id in data:
                self.write(data.get(id), id=id)
        else:
            self.write(data)
