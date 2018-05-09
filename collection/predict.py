from chalicelib import *
from optparse import OptionParser
import dateutil
import json
import pandas as pd
import pandas
import numpy as np
import datetime
from scipy import stats
from math import ceil, isnan

from sklearn.linear_model import LinearRegression
from sklearn import preprocessing, cross_validation, svm


def predict(data, days):
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


config = ConfigStage('chalicelib/collection.ini')

minutes = int(config.get("main", "minutes", 60))
input_type = config.get("main", "input", "api")
output_type = config.get("main", "output", "file")
pretty = bool(config.get("main", "pretty", False))

outfile = config.get("file", "outfile", "output.json")

elastic_dict = config.items("elastic", {})
elastic_url = elastic_dict.pop("url", "localhost")

index_dict = config.items("history_index", {})
index = index_dict.pop("name", "spot_price_history")
doc_type = index_dict.pop("doc_type", "price")
mappings = json.loads(index_dict.pop("mappings", "{}"))

instance_index_dict = config.items("instance_index", {})
instance_index = instance_index_dict.pop("name", "instance_map")
instance_doc_type = instance_index_dict.pop("doc_type", "instance")
instance_mappings = json.loads(instance_index_dict.pop("mappings", "{}"))

bid_index_dict = config.items("bid_index", {})
bid_index = bid_index_dict.pop("name", "spot_bids")
bid_doc_type = bid_index_dict.pop("doc_type", "bid")
bid_mappings = json.loads(bid_index_dict.pop("mappings", "{}"))

opt_parser = OptionParser()
opt_parser.add_option("--pretty", "-p", action="store_true", dest="pretty", default=pretty,
                      help="Pretty format output")
opt_parser.add_option("--elasticurl", "-e", action="store", type="string", dest="elastic_url", default=elastic_url,
                      help="URL for the elasticsearch server (Default: {})".format(elastic_url))

(options, args) = opt_parser.parse_args()
elastic_url = options.elastic_url.split(',')


history_index = IndexData(elastic_url, index, doc_type=doc_type, connection_options=elastic_dict, index_settings=index_dict, index_mappings=mappings)
instance_index = IndexData(elastic_url, instance_index, doc_type=instance_doc_type, connection_options=elastic_dict,
                           index_settings=instance_index_dict, index_mappings=instance_mappings, alias=True)
bid_index = IndexData(elastic_url, bid_index, doc_type=bid_doc_type, connection_options=elastic_dict,
                      index_settings=bid_index_dict, index_mappings=bid_mappings)
instances = InstanceMap(elastic_index=instance_index, ttl=8640000)


eprint("Training")
for instance in instances.get_types():
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

    for region in instance_az_history:
        estimates = {}
        for os in instance_az_history[region]:
            for az in instance_az_history[region][os]:
                estimates[az] = []
                for days in range(1, 30):
                    estimates[az].append(predict(instance_az_history[region][os][az], days))

                if max(estimates[az]) == 999:
                    estimates.pop(az)
                else:
                    estimates[az] = [999 if isnan(x) else x for x in estimates[az]]

                summary = []
                az_max = {}
                for day in range(0,29):
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
                    bid_index.write(estimates, "{}~{}~{}".format(region, instance, os))



