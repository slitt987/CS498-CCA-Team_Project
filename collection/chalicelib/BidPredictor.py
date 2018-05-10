import pandas as pd
import pandas
import numpy as np
from scipy import stats
from math import ceil, isnan

from sklearn.linear_model import LinearRegression
from sklearn import preprocessing, cross_validation
from threading import Thread
from common import eprint
import dateutil


class BidPredictor:
    def __init__(self, history_index, bid_index, n_days = 30):
        self.__history_index = history_index
        self.__bid_index = bid_index
        self.n_days = n_days

    def thread_process_instance(self, instances):
        """
        Returns a training thread
        :param instances: list of instances to train
        :return: thread
        """
        thread = Thread(target=self.process_instances, args=(instances, self.__history_index,))
        thread.start()
        return thread

    def process_instances(self, instances, history_index):
        """
        Models a set of instances and writes results to ES
        :param instances: list of instances to train
        :param history_index:
        :return:
        """
        [self.model_instance(instance, history_index) for instance in instances]

    @staticmethod
    def get_bid_es_key(region, instance, os):
        """ Generates an ElasticSearch document id for input """
        return "{}~{}~{}".format(region, instance, os)

    def get_bid(self, region, instance, os, duration):
        """
        Gets the bid from ElasticSearch for the given parameters
        :param region:
        :param instance:
        :param os:
        :param duration:
        :return: Pair [az, bid] - returns [None, -1] on not found/error
        """
        if duration < 1:
            duration = 1

        bid = self.__bid_index.get_doc(self.get_bid_es_key(region, instance, os))
        if bid is None:
            return [None, -1]
        else:
            summary = bid.get("summary")
            n_days = len(summary)
            if duration > n_days:
                duration = n_days
            if self.n_days != n_days:
                self.n_days = n_days

            result = summary[duration - 1].split('/')
            if len(result) != 2:
                eprint("Internal Error - Malformed Bid data for request: {}, {}, {}, {}".format(region, instance,
                                                                                                os, duration))
                return [None, -1]
            else:
                return result

    @staticmethod
    def predict(data, days):
        """
        Generates a prediction for the given timeseries
        :param data: a dataframe to train on 2 fields: Date, Price
        :param days: Number of days out to predict
        :return: predicted price
        """
        df = pd.DataFrame(data)
        df = df[['Price']]
        forecast_out = int(days)  # predicting 30 days into future
        df['Prediction'] = df[['Price']].shift(-forecast_out)
        X = np.array(df.drop(['Prediction'], 1))
        X = preprocessing.scale(X)
        X_forecast = X[-forecast_out:]  # set X_forecast equal to last 30
        X = X[:-forecast_out]  # remove last 30 from X
        y = np.array(df['Prediction'])
        y = y[:-forecast_out]
        X_train, X_test, y_train, y_test = cross_validation.train_test_split(X, y, test_size=0.2)
        # Training
        clf = LinearRegression()
        if len(X) == 0:
            return 999
        else:
            clf.fit(X, y)
            # Testing
            confidence = clf.score(X_test, y_test)
            forecast_prediction = clf.predict(X_forecast)
            return ceil(float(stats.gmean(forecast_prediction)) * 1000) / 1000

    @staticmethod
    def split_data(instance_history):
        """
        Reads the data for a given instance and converts the data to complex dictionary for training
        :param instance_history: a list of data (dictionaries), fields: Region, OS, AvailabilityZone, Date, Price
        :return: Multilevel dictionary of Rows (dictionaries), Levels: region, os, az, fields: Date, Price
        """
        instance_az_history = {}
        for h in instance_history:
            region = h.pop("Region")
            os = h.pop("OS")
            az = h.pop("AvailabilityZone")
            if region in instance_az_history:
                if os in instance_az_history[region]:
                    if az in instance_az_history[region][os]:
                        instance_az_history[region][os][az].append(h)
                    else:
                        instance_az_history[region][os][az] = [h]
                else:
                    instance_az_history[region][os] = {az: [h]}
            else:
                instance_az_history[region] = {os: {az: [h]}}

        return instance_az_history

    def model_data(self, instance, region, os, data):
        """
        Models data for a given set of input parameters and writes results to ES
        :param instance:
        :param region:
        :param os:
        :param data: list of fields (dictionary), fields: Date, Price
        :return: None
        """
        estimates = {}
        for az in data:
            estimates[az] = []
            for days in range(1, self.n_days + 1):
                estimates[az].append(self.predict(data[az], days))

            if max(estimates[az]) == 999:
                estimates.pop(az)
            else:
                estimates[az] = [999 if isnan(x) else x for x in estimates[az]]

        summary = []
        az_max = {}
        for day in range(0, self.n_days):
            min_az = None
            min_price = -1
            for az in estimates:
                az_max[az] = max(az_max.get(az, -1), estimates[az][day])
                if min_az is None or az_max[az] < min_price:
                    min_az = az
                    min_price = az_max[az]

            if min_az is not None:
                summary.append("{}/{}".format(min_az, min_price))
            else:
                summary.append("{}/{}".format(min_az, 999))

        if len(estimates) > 0:
            estimates["summary"] = summary

            eprint("Trained: {}, {}, {}".format(region, instance, os))
            self.__bid_index.write(estimates, self.get_bid_es_key(region, instance, os))

    def model_instance(self, instance, history_index):
        """
        Runs all the necessary steps to model a given AWS instance type and write the results to ES
        :param instance:
        :param history_index:
        :return:
        """
        eprint("Fetching data for: {}".format(instance))
        instance_history = history_index.search_terms({"InstanceType": instance})
        instance_history = (
            {
                "Region": h.get("Region"),
                "Date": pandas.to_datetime(dateutil.parser.parse(h.get("Timestamp")).strftime("%Y-%m-%d")),
                "OS": h.get("ProductDescription").lower(),
                "Price": float(h.get("SpotPrice")),
                "AvailabilityZone": h.get("AvailabilityZone")
            } for h in instance_history)

        instance_az_history = self.split_data(instance_history)

        for region in instance_az_history:
            for os in instance_az_history[region]:
                self.model_data(instance, region, os, instance_az_history[region][os])
