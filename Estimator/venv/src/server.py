import logging
import os
# import numpy as np
import pandas as pd
import pickle
from xgboost import XGBClassifier
from flask import Flask, jsonify, request, Response

import database_module
import engineering_module

server = Flask(__name__)


@server.route('/get_prediction', methods=['POST'])
def get_prediction():
    if estimator_holder.enabled:
        if len(estimator_holder.pipeline_and_feature_engineering_by_id) == 0:
            response = Response("No Pipeline loaded", status=500)
            logger.error("No Pipeline loaded")
        elif estimator_holder.currencyEncoder is None or estimator_holder.countryEncoder is None:
            response = Response("No Country or Currency encoder loaded", status=500)
        elif estimator_holder.is_feature_engineering_required and estimator_holder.feature_engineering_database is None:
            response = Response("No feature engineering database", status=500)
        else:
            request_data = request.get_json()
            if request_data is None or len(request_data) == 0:
                response = Response("No input data", status=400)
            else:
                result = estimator_holder.predict(request_data)
                response = jsonify(result)
    else:
        response = Response("Estimator application is disabled", status=500)
        logger.error("Estimator application is disabled, can't predict")
    return response


@server.route('/set_feature_engineering_database', methods=['POST'])
def set_feature_engineering_database():
    input_data = request.get_json()
    if not database_handler.is_database_exist(input_data):
        message = f"Feature engineering database doesn't exist, database name: {input_data}"
        response = Response(message, status=500)
        logger.error(message)
    elif not database_handler.is_feature_engineered_table_exist(input_data):
        message = f"Feature engineering table doesn't exist, database name: {input_data}"
        response = Response(message, status=500)
        logger.error(message)
    else:
        estimator_holder.feature_engineering_database = input_data
        response = Response(status=200)
        logger.info(f"Feature engineering database set, database name: {input_data}")
    return response


@server.route('/api/v1/pipeline', methods=['POST'])
def add_pipeline():
    estimator_id_collection = request.get_json()
    installed_estimator_id_collection = estimator_holder.pipeline_and_feature_engineering_by_id.keys()
    for id in estimator_id_collection:
        if id not in installed_estimator_id_collection:
            pickled_estimator_container = database_handler.load_estimator_container(id)
            if pickled_estimator_container is None:
                message = f"Estimator with given id doesn't exist, id: {id}"
                response = Response(message, status=400)
                logger.error(message)
            else:
                estimator_container = pickle.loads(pickled_estimator_container)
                pipeline = estimator_container.get("pipeline")
                encoders = estimator_container.get("encoders")
                is_feature_engineering_required = estimator_container.get("feature_engineering_field")
                pipeline_and_feature_engineering_list = list()
                pipeline_and_feature_engineering_list.append(pipeline)
                pipeline_and_feature_engineering_list.append(is_feature_engineering_required)
                response = estimator_holder.add_pipeline(id, pipeline_and_feature_engineering_list)
                if estimator_holder.countryEncoder is None:
                    estimator_holder.countryEncoder = encoders.get("country_encoder")
                    logger.info("Country Encoder loaded")
                if estimator_holder.currencyEncoder is None:
                    estimator_holder.currencyEncoder = encoders.get("currency_encoder")
                    logger.info("Currency Encoder loaded")
                if is_feature_engineering_required:
                    estimator_holder.is_feature_engineering_required = True
                logger.info(f"Pipeline loaded, id: {id}")
        else:
            message = f"The given pipeline is installed yet, estimator id: {id}"
            logger.info(message)
            response = estimator_holder.get_all_pipeline_id()
    return response


@server.route('/api/v1/pipeline/<id>', methods=['DELETE'])
def delete_pipeline(id):
    return estimator_holder.delete_pipeline(id)


@server.route('/api/v1/pipeline', methods=['GET'])
def get_pipeline():
    result = estimator_holder.get_all_pipeline_id()
    logger.info(f"Installed pipeline ids: {result}")
    return result


@server.route('/enabling_prediction', methods=['POST'])
def enable_pipeline():
    input = request.get_json()
    if input is None:
        response = Response("Empty HTTP body", status=400)
        logger.error("Empty HTTP body")
    elif input == "enable_prediction":
        estimator_holder.enable_prediction()
        response = Response(status=200)
        logger.info("Prediction enabled")
    elif input == "disable_prediction":
        estimator_holder.disable_prediction()
        response = Response(status=200)
        logger.info("Prediction disabled")
    else:
        response = Response("Wrong command", status=400)
        logger.error("Wrong command")
    return response

class CommonComponents:
    def __init__(self):
        pass
        

class SingleEstimator:
    def __int__(self):
        self.pipeline = None


class EstimatorHolder:
    def __init__(self):
        self.pipeline_and_feature_engineering_by_id = dict()
        self.encoder_object_by_field = dict()
        self.currencyEncoder = None
        self.countryEncoder = None
        self.enabled = False
        self.is_feature_engineering_required = False
        self.feature_engineering_database = None

    def add_pipeline(self, id, pipeline_and_feature_engineering):
        self.pipeline_and_feature_engineering_by_id[id] = pipeline_and_feature_engineering
        logger.debug(f"Pipeline added, id: {id}")
        return jsonify(tuple(self.pipeline_and_feature_engineering_by_id.keys()))

    def delete_pipeline(self, id):
        id_num = int(id)
        if id_num in self.pipeline_and_feature_engineering_by_id.keys():
            self.pipeline_and_feature_engineering_by_id.pop(id_num)
            message = f"Pipeline uninstalled, pipeline_id: {id_num}"
            logger.info(message)
            response = jsonify(tuple(self.pipeline_and_feature_engineering_by_id.keys()))
        else:
            message = f"Pipeline does not installed, pipeline id: {id_num}"
            response = Response(message, status=400)
            logger.info(message)
        return response

    def get_all_pipeline_id(self):
        return jsonify(tuple(self.pipeline_and_feature_engineering_by_id.keys()))

    def enable_prediction(self):
        self.enabled = True
        logger.debug("Estimation enabled")

    def disable_prediction(self):
        self.enabled = False
        logger.debug("Estimation disabled")

    def set_feature_engineering_database(self, database_name):
        self.feature_engineering_database = database_name
        logger.info(f"Feature engineering database set, database name: {database_name}")

    def predict(self, input_data):
        result = dict()
        encoded_input = self.encode_raw_input(input_data)
        feature_engineered_input = None
        if self.is_feature_engineering_required:
            given_card_number = encoded_input[0]
            given_timestamp = encoded_input[2]
            earlier_amount_and_timestamp_collection = database_handler.get_earlier_amount_and_time_stamp_collection(
                self.feature_engineering_database, given_card_number,
                given_timestamp)

            feature_engineer = engineering_module.Engineer()
            feature_engineered_input = feature_engineer.get_engineered_transaction(encoded_input,
                                                                                   earlier_amount_and_timestamp_collection)
            # database_handler.save_feature_engineered_dataset(self.feature_engineering_database,
            #                                                  feature_engineered_input)

        for id, pipeline_and_feature_engineering_properties in self.pipeline_and_feature_engineering_by_id.items():
            pipeline = pipeline_and_feature_engineering_properties[0]
            is_feature_engineering_required = pipeline_and_feature_engineering_properties[1]
            if is_feature_engineering_required:
                current_prediction = pipeline.predict([feature_engineered_input])
                current_probability = pipeline.predict_proba([feature_engineered_input])
                logger.info("Prediction with feature engineering")
            else:
                current_prediction = pipeline.predict([encoded_input])
                current_probability = pipeline.predict_proba([encoded_input])
                logger.info("Prediction without feature engineering")
            prediction_properties = {
                'prediction': int(current_prediction[0]),
                'negativeProbability': float(current_probability[0][0]),
                'positiveProbability': float(current_probability[0][1])
            }
            logger.info(
                f"Pipeline id: {id}, prediction: {current_prediction[0]}, positive probability: {float(current_probability[0][1])}, negative probability: {float(current_probability[0][0])}")
            result[id] = prediction_properties
        # response=dict()
        # response["response"]=result
        return result

    def encode_raw_input(self, input):
        card_number = input.get("card_number")
        transaction_type = input.get("transaction_type")
        amount = input.get('amount')
        currency_name_string = input.get("currency_name")
        currency_name = self.currencyEncoder.transform([currency_name_string])[0]
        response_code = input.get("response_code")
        country_name_string = input.get("country_name")
        transformed_country_name_string = [country_name_string]
        country_nane = self.countryEncoder.transform([country_name_string])[0]
        vendor_code = input.get("vendor_code")
        year = input.get("year")
        month = input.get("month")
        day = input.get("day")
        hour = input.get("hour")
        min = input.get("min")
        sec = input.get("sec")
        micro = input["micro"]
        ts = pd.Timestamp(year, month, day, hour, min, sec, micro)
        julian_date = ts.to_julian_date()
        return (
            card_number, transaction_type, julian_date, amount, currency_name, response_code, country_nane, vendor_code)


if __name__ == "__main__":
    log_file = os.getenv("LOGFILE", "C:/Temp/Estimator.log")
    log_level = os.getenv("LOG_LEVEL", "INFO")
    database_url = os.getenv("MYSQL_DATABASE_URL", "localhost")
    database_user = os.getenv("MYSQL_USER", "root")
    database_password = os.getenv("MYSQL_PASSWORD", "TOmi1970")
    file_handler = logging.FileHandler(log_file)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    logger = logging.getLogger("estimator.server")
    logger.setLevel(log_level)
    logger.addHandler(file_handler)
    logger.info("MYSQL_DATABASE_URL: " + database_url)
    logger.info("MYSQL_USER :" + database_user)
    logger.info("MYSQL_PASSWORD: " + database_password)
    database_handler = database_module.Handler(database_url, database_user, database_password)
    estimator_holder = EstimatorHolder()
    logger.info("Estimator application started")

    server.run(host='0.0.0.0', port=8083, debug=True)
