import numpy as np
import pandas as pd
import scipy.stats as stats
import sklearn
import glob, os
from common import eprint
from sklearn.linear_model import LinearRegression


class SpotBidPredictor:

    _lm_model = None
    _az_category_code = None
    _os_category_code = None
    _type_category_code = None

    def __init__(self, data_generator):
        # Init a predictor
        eprint("Converting data to DataFrame")
        self._int_df = pd.DataFrame(data_generator)
        eprint("Setting Target")
        self._target_price = self._int_df['SpotPrice']
        eprint("Pre-Processing")
        self._pre_process_train_data()
        eprint("Training")
        self.train()

    def train(self):
        self._lm_model = LinearRegression()
        X = self._train_data.drop('SpotPrice', axis=1)
        Y = self._target_price
        self._lm_model.fit(X,Y)

    # Sample input dict
    # {'InstanceType':['c3.8xlarge'], 'Region':['ap-northeast-1a'], 'ProductDescription':['Windows']}
    def predict(self, instance_type, region, product_description):
        type_code = self.get_type_cat_code(instance_type)
        region_code = self.get_az_cat_code(region)
        os_code = self.get_os_cat_code(product_description)
        input = self._get_df_from_values(type_code, region_code, os_code)
        return self._lm_model.predict(input)[0]

    def get_train_data(self):
        return self._train_data

    def get_target_bid_price(self):
        return self._target_price

    def get_az_cat_code(self, value):
        return self._get_key_from_value(self._az_category_code, value);

    def get_os_cat_code(self, value):
        return self._get_key_from_value(self._os_category_code, value);

    def get_type_cat_code(self, value):
        return self._get_key_from_value(self._type_category_code, value);

    def _pre_process_train_data(self):
        self._train_data = self._int_df
        self._categorize_train_data()
        self._prune_train_data()

    def _get_key_from_value(self, data, value):
        for code, name in data.iteritems():
            if value == name:
                return code
        return None

    def _get_df_from_values(self, type_code, region_code, os_code):
        temp = {'InstanceType':[type_code], 'Region':[region_code], 'ProductDescription':[os_code]}
        print ("Created DF for predict ", temp)
        return pd.DataFrame(temp)

    def _categorize_train_data(self):
        self._train_data['InstanceType'] = self._train_data['InstanceType'].astype('category')
        self._train_data['ProductDescription'] = self._train_data['ProductDescription'].astype('category')
        self._train_data['Region'] = self._train_data['Region'].astype('category')
        self._train_data['OS'] = self._train_data['ProductDescription'].cat.codes
        self._train_data['LOC'] = self._train_data['Region'].cat.codes
        self._train_data['TYPE'] = self._train_data['InstanceType'].cat.codes
        self._az_category_code = dict(enumerate(self._train_data['Region'].cat.categories))
        self._os_category_code = dict(enumerate(self._train_data['ProductDescription'].cat.categories))
        self._type_category_code = dict(enumerate(self._train_data['InstanceType'].cat.categories))

    def _prune_train_data(self):
        #self._train_data = self._train_data.drop('Timestamp', axis=1)
        self._train_data = self._train_data.drop('InstanceType', axis=1)
        self._train_data = self._train_data.drop('Region', axis=1)
        self._train_data = self._train_data.drop('ProductDescription', axis=1)

# Unit Testing predict
def get_test_generator():
    data = [
        {'InstanceType':'c3.8xlarge', 'Region':'ap-northeast-1a', 'ProductDescription':'Windows', 'SpotPrice' : 1.6503},
        {'InstanceType':'c3.8xlarge', 'Region':'ap-northeast-1a', 'ProductDescription':'Windows', 'SpotPrice' : 1.6503},
        {'InstanceType':'c3.8xlarge', 'Region':'ap-northeast-1a', 'ProductDescription':'Windows', 'SpotPrice' : 1.6503},
        {'InstanceType':'c3.8xlarge', 'Region':'ap-northeast-1a', 'ProductDescription':'Windows', 'SpotPrice' : 1.6503},
        {'InstanceType':'c3.8xlarge', 'Region':'ap-northeast-1a', 'ProductDescription':'Windows', 'SpotPrice' : 1.6503},
        {'InstanceType':'c3.8xlarge', 'Region':'ap-northeast-1a', 'ProductDescription':'Windows', 'SpotPrice' : 1.6503},
        {'InstanceType':'c3.8xlarge', 'Region':'ap-northeast-1a', 'ProductDescription':'Windows', 'SpotPrice' : 1.6503},
        {'InstanceType':'c3.8xlarge', 'Region':'ap-northeast-1a', 'ProductDescription':'Windows', 'SpotPrice' : 1.6503},
        {'InstanceType':'c3.8xlarge', 'Region':'ap-northeast-1a', 'ProductDescription':'Windows', 'SpotPrice' : 1.6503},
        {'InstanceType':'c3.8xlarge', 'Region':'ap-northeast-1a', 'ProductDescription':'Windows', 'SpotPrice' : 1.6503},
        {'InstanceType':'c3.8xlarge', 'Region':'ap-northeast-1a', 'ProductDescription':'Windows', 'SpotPrice' : 1.6503},
        {'InstanceType':'c3.8xlarge', 'Region':'ap-northeast-1a', 'ProductDescription':'Windows', 'SpotPrice' : 1.6503},
        {'InstanceType':'c3.8xlarge', 'Region':'ap-northeast-1a', 'ProductDescription':'Windows', 'SpotPrice' : 1.6503},
        {'InstanceType':'c3.8xlarge', 'Region':'ap-northeast-1a', 'ProductDescription':'Windows', 'SpotPrice' : 1.6503},]
    return (n for n in data)

