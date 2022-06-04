import math
import pickle
import time
from threading import Thread

from flask import Flask, jsonify, request, Response
import pika
import json
import os
import logging
from datetime import datetime
from lightgbm import LGBMClassifier
from sklearn.discriminant_analysis import LinearDiscriminantAnalysis
from sklearn.ensemble import RandomForestClassifier
from sklearn.feature_selection import RFECV
from sklearn.decomposition import PCA, TruncatedSVD
from sklearn.linear_model import LogisticRegression
from sklearn.naive_bayes import GaussianNB
from sklearn.neighbors import KNeighborsClassifier
from sklearn.pipeline import Pipeline
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler, QuantileTransformer, Normalizer, RobustScaler, MaxAbsScaler, \
    MinMaxScaler
from sklearn.tree import DecisionTreeClassifier
from xgboost import XGBClassifier
from sklearn.utils.validation import column_or_1d
from imblearn.combine import SMOTEENN
from imblearn.over_sampling import RandomOverSampler
from imblearn.under_sampling import RandomUnderSampler
from sklearn.metrics import confusion_matrix, accuracy_score, balanced_accuracy_score, recall_score, f1_score, \
    roc_auc_score
import database_module

app = Flask(__name__)


class NoFraudTypeFieldException(Exception):
    pass


class TooManyFraudTypeFieldException(Exception):
    pass


class NotEnoughParameterException(Exception):
    pass


class FieldNotExistInTableException(Exception):
    pass


class EncoderNotExistExceptiom(Exception):
    pass


class FieldNotApplicableForFraudException(Exception):
    pass


class FieldNotApplicableForFeatureEngineeringException(Exception):
    pass


def send_message(response):
    try:
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=rabbitmq_url, port=5672))
        channel = connection.channel()
        channel.queue_declare(queue='train.queue', durable=True)
        channel.basic_publish(exchange='', routing_key='train.queue', body=json.dumps(response))
        connection.close()
    except (pika.exceptions.AMQPConnectionError, pika.exceptions.AMQPChannelError) as error:
        logger.error(f"RabbitMQ error: {error}")


def get_feature_selectors(cpu_core_num=1):
    feature_selectors = {
        1: ('RFE', RFECV(estimator=RandomForestClassifier(n_jobs=cpu_core_num), n_jobs=cpu_core_num)),
        2: ('PCA', PCA(n_components=0.95, svd_solver='full')),
        3: ('SVD', TruncatedSVD())
    }
    return feature_selectors


def get_samplers(cpu_core_num=1):
    samplers = {
        1: ('RandomUnderSampler', RandomUnderSampler(sampling_strategy=0.5)),
        2: ('RandomOverSampler', RandomOverSampler(sampling_strategy=0.5)),
        3: ('SMOTEENN', SMOTEENN(sampling_strategy=0.5, n_jobs=cpu_core_num))
    }
    return samplers


def get_scalers():
    scalers = {
        1: ('StandardScaler', StandardScaler()),
        2: ('MinMaxScaler', MinMaxScaler()),
        3: ('MaxAbsScaler', MaxAbsScaler()),
        4: ('RobustScaler', RobustScaler()),
        5: ('QuantileTransformer-Normal', QuantileTransformer(output_distribution='normal')),
        6: ('QuantileTransformer-Uniform', QuantileTransformer(output_distribution='uniform')),
        7: ('Normalizer', Normalizer()),
    }
    return scalers


def get_models(cpu_core_num=1):
    models = {
        1: ('Logistic Regression', LogisticRegression(n_jobs=cpu_core_num)),
        2: ('LinearDiscriminantAnalysis', LinearDiscriminantAnalysis()),
        3: ('K-Nearest Neighbor', KNeighborsClassifier(n_jobs=cpu_core_num)),
        4: ('DecisionTree', DecisionTreeClassifier()),
        5: ('GaussianNB', GaussianNB()),
        # 'SupportVectorMachine GPU': SupportVectorMachine(use_gpu=True),
        # 'Random Forest GPU': RandomForestClassifier(use_gpu=True, gpu_ids=[0, 1], use_histograms=True),
        6: ('Random Forest', RandomForestClassifier(n_jobs=cpu_core_num)),
        # 'MLP': MLPClassifier(),
        7: ('Light GBM', LGBMClassifier(n_jobs=cpu_core_num)),
        # 'XGBoost': XGBClassifier(tree_method='gpu_hist', gpu_id=0)
        8: ('XGBoost', XGBClassifier())
    }
    return models


@app.route('/encoding_plan', methods=['POST'])
def save_encoding_plan():
    request_parameter = request.get_json()
    if request_parameter is None:
        error_message = "Encoding plan POST body is empty"
        logger.error(error_message)
        return Response(error_message, status=400)
    schema_name = request_parameter.get("schema_name")
    if schema_name is None:
        error_message = "Encoding plan's schema name is missing"
        logger.error(error_message)
        return Response(error_message, status=400)
    table_name = request_parameter.get("table_name")
    if table_name is None:
        error_message = "Encoding plan's table name is missing"
        logger.error(error_message)
        return Response(error_message, status=400)
    encoding_parameters = request_parameter.get("encoding_parameters")
    if encoding_parameters is None:
        error_message = "Encoding plan's encoding parameter is missing"
        logger.error(error_message)
        return Response(error_message, status=400)
    detailed_information_about_table = database_handler.get_detailed_information_about_table(schema_name=schema_name,
                                                                                             table_name=table_name)
    field_and_type_and_other_eligible_type_for_encoding_collection = detailed_information_about_table.get("fields")
    fraud_candidates = detailed_information_about_table.get("fraud_candidates")
    try:
        check_fields_and_possible_encoding(field_and_type_and_other_eligible_type_for_encoding_collection,
                                           fraud_candidates, encoding_parameters)
    except NotEnoughParameterException as e:
        details = e.args[0]
        message = details.get("message")
        parameter_name = details.get("parameter_name")
        error_message = f"{message}, parameter: {parameter_name}"
        logger.error(error_message)
        return Response(error_message, status=400)
    except FieldNotExistInTableException as e:
        details = e.args[0]
        message = details.get("message")
        field_name = details.get("field_name")
        error_message = f"{message}, schema name: {schema_name}, table name: {table_name}, field name: {field_name}"
        logger.error(error_message)
        return Response(error_message, status=400)
    except FieldNotApplicableForFraudException as e:
        details = e.args[0]
        message = details.get("message")
        field_name = details.get("field_name")
        error_message = f"{message}, schema name: {schema_name}, table name: {table_name}, field name: {field_name}"
        logger.error(error_message)
        return Response(error_message, status=400)
    except EncoderNotExistExceptiom as e:
        details = e.args[0]
        message = details.get("message")
        field_name = details.get("field_name")
        planned_encoding_type = details.get("planned_encoding_type")
        error_message = f"{message}, schema name: {schema_name}, table name: {table_name}, field name: {field_name}, planned encoding type: {planned_encoding_type}"
        logger.error(error_message)
        return Response(error_message, status=400)
    except NoFraudTypeFieldException as e:
        error_message = "There aren't any fraud type field in encoding parameters"
        logger.error(error_message)
        return Response(error_message, status=400)
    except TooManyFraudTypeFieldException as e:
        error_message = "There are too many fraud type field in encoding parameters"
        logger.error(error_message)
        return Response(error_message, status=400)
    last_row_id = database_handler.persist_encoding_plan(schema_name, table_name, encoding_parameters)
    return jsonify(last_row_id)


def check_fields_and_possible_encoding(field_and_type_and_other_eligible_type_for_encoding_collection, fraud_candidates,
                                       encoding_parameters):
    number_of_int_and_float_typed_fields = get_number_of_int_and_float_typed_fields(
        field_and_type_and_other_eligible_type_for_encoding_collection)
    fields_as_keys_in_encoding_parameters = encoding_parameters.keys()
    number_of_encoding_parameters = len(fields_as_keys_in_encoding_parameters)
    number_of_fields_in_current_table = len(field_and_type_and_other_eligible_type_for_encoding_collection)
    if number_of_encoding_parameters < number_of_fields_in_current_table - number_of_int_and_float_typed_fields:
        raise NotEnoughParameterException({"message": "Not enough parameter", "parameter_name": "encoding_parameters"})
    field_names_in_field_and_type_properties = get_field_names_from_field_and_type_properties(
        field_and_type_and_other_eligible_type_for_encoding_collection)
    for field in fields_as_keys_in_encoding_parameters:
        if field not in field_names_in_field_and_type_properties:
            raise FieldNotExistInTableException(
                {"message": "The field doesn't exist in this database", "field_name": field})
        if encoding_parameters.get(field) == "fraud_type":
            check_if_field_applicable_for_fraud(fraud_candidates, field)

    all_field_names = list()
    for item in field_and_type_and_other_eligible_type_for_encoding_collection:
        field_name = item.get("name")
        all_field_names.append(field_name)
        if encoding_parameters.get(field_name) is not None:
            planned_encoding_type = encoding_parameters.get(field_name)
            if (planned_encoding_type == "julian" and item.get(
                    "type") != "datetime") or (planned_encoding_type == "label_encoder" and item.get(
                "type") != "varchar") or (planned_encoding_type == "int" and "int" not in item.get(
                "other_eligible_type_for_encoding")) or (planned_encoding_type == "float" and "float" not in item.get(
                "other_eligible_type_for_encoding")) or planned_encoding_type not in ["julian", "label_encoder", "int",
                                                                                      "float", "fraud_type"]:
                raise EncoderNotExistExceptiom(
                    {"message": "The planned encoding can not applicable to this field", "field_name": field_name,
                     "planned_encoding_type": planned_encoding_type})
    check_number_of_fraud_type_field(all_field_names, encoding_parameters)


def get_number_of_int_and_float_typed_fields(field_and_type_and_other_eligible_type_for_encoding_collection):
    int_types = ("tinyint", "smallint", "mediumint", "int", "bigint")
    float_types = ("float", "double")
    number_of_int_and_float_typed_field = 0
    for field_property in field_and_type_and_other_eligible_type_for_encoding_collection:
        current_type = field_property.get("type")
        if current_type in int_types or current_type in float_types:
            number_of_int_and_float_typed_field += 1
    return number_of_int_and_float_typed_field


def check_if_field_applicable_for_fraud(fraud_candidates, current_field_name):
    applicable_field_names_for_fraud = list()
    for item in fraud_candidates:
        applicable_field_names_for_fraud.append(item.get("name"))
    if current_field_name not in applicable_field_names_for_fraud:
        raise FieldNotApplicableForFraudException(
            {"message": "The field not applicable for fraud", "field_name": current_field_name})


def check_number_of_fraud_type_field(fields, encoding_parameters):
    number_of_fraud_type_field = 0
    for field in fields:
        if encoding_parameters.get(field) == "fraud_type":
            number_of_fraud_type_field += 1
    if number_of_fraud_type_field == 0:
        raise NoFraudTypeFieldException()
    if number_of_fraud_type_field > 1:
        raise TooManyFraudTypeFieldException()


def get_field_names_from_field_and_type_properties(field_and_type_properties):
    result = list()
    for item in field_and_type_properties:
        result.append(item.get("name"))
    return result


@app.route('/encoding_plan', methods=['GET'])
def get_encoded_table():
    id = request.args.get("id")
    try:
        if id is None:
            result = database_handler.get_all_encoding_plan()
        else:
            result = database_handler.get_encoding_plan_by_id(id)
    except database_module.NoneException as e:
        details = e.args[0]
        message = details.get("message")
        parameter = details.get("parameter")
        error_message = f"{message}, id: {parameter}"
        logger.error(error_message)
        return Response(error_message, status=400)
    return jsonify(result)


@app.route('/feature_engineering_plan', methods=['POST'])
def save_feature_engineering_plan():
    request_parameter = request.get_json()
    if request_parameter is None:
        error_message = "Feature engiennering plan POST body is empty"
        logger.error(error_message)
        return Response(error_message, status=400)
    schema_name = request_parameter.get("schema_name")
    if schema_name is None:
        error_message = "Feature engineering plan's schema name is missing"
        logger.error(error_message)
        return Response(error_message, status=400)
    table_name = request_parameter.get("table_name")
    if table_name is None:
        error_message = "Feature engineering plan's table name is missing"
        logger.error(error_message)
        return Response(error_message, status=400)
    feature_engineering_parameters = request_parameter.get("feature_engineering_parameters")
    if feature_engineering_parameters is None:
        error_message = "Feature engineering plan's parameters are missing"
        logger.error(error_message)
        return Response(error_message, status=400)
    feature_name = feature_engineering_parameters.get("feature_name")
    if feature_name:
        error_message = "Feature engineering plan's feature name in parameters is missing"
        logger.error(error_message)
        return Response(error_message, status=400)
    intervals = feature_engineering_parameters.get("intervals")
    if intervals is None or not intervals:
        error_message = "Feature engineering plan's intervals in parameters is missing"
        logger.error(error_message)
        return Response(error_message, status=400)
    detailed_information_about_table = database_handler.get_detailed_information_about_table(schema_name=schema_name,
                                                                                             table_name=table_name)
    field_and_type_and_other_eligible_type_for_encoding_collection = detailed_information_about_table.get("fields")
    try:
        check_if_field_applicable_for_feature_engineering(
            field_and_type_and_other_eligible_type_for_encoding_collection, feature_name)
    except FieldNotApplicableForFeatureEngineeringException as e:
        details = e.args[0]
        message = details.get("message")
        field_name = details.get("field_name")
        error_message = f"{message}, schema name: {schema_name}, table name: {table_name}, field name: {field_name}"
        logger.error(error_message)
        return Response(error_message, status=400)
    last_row_id = database_handler.persist_feature_engineering_plan(schema_name, table_name,
                                                                    feature_engineering_parameters)
    return jsonify(last_row_id)


def check_if_field_applicable_for_feature_engineering(field_and_type_and_other_eligible_type_for_encoding_collection,
                                                      feature_name):
    field_names_in_field_and_type_properties = get_field_names_from_field_and_type_properties(
        field_and_type_and_other_eligible_type_for_encoding_collection)
    if feature_name not in field_names_in_field_and_type_properties:
        raise FieldNotExistInTableException(
            {"message": "The field doesn't exist in this database", "field_name": feature_name})


@app.route('/feature_engineering_plan', methods=['GET'])
def get_feature_engineering_plan():
    id = request.args.get("id")
    if id is None:
        result = database_handler.get_all_feature_engineering_plan()
    else:
        result = database_handler.get_feature_engineering_plan_by_id(id)
    return jsonify(result)


@app.route('/train_task', methods=['POST'])
def save_train_task():
    feature_selector_keys = get_feature_selectors().keys()
    sampler_keys = get_samplers().keys()
    scaler_keys = get_scalers().keys()
    model_keys = get_models().keys()
    request_parameter = request.get_json()
    if request_parameter is None:
        error_message = "Train task POST body is empty"
        logger.error(error_message)
        return Response(error_message, status=400)
    try:
        result = database_handler.persist_train_task(request_parameter, feature_selector_keys, sampler_keys,
                                                     scaler_keys, model_keys)
    except database_module.IllegalArgumentException as e:
        details = e.args[0]
        message = details.get("message")
        request_parameter = details.get("request_parameter")
        error_message = f"{message}, request parameter: {request_parameter}"
        logger.error(error_message)
        return Response(error_message, status=400)
    return jsonify(result)


@app.route('/train_task', methods=['GET'])
def get_train_task():
    id = request.args.get("id")
    if id is None:
        result = database_handler.get_all_train_task()
    else:
        result = database_handler.get_train_task_by_id(id)
    return jsonify(result)


@app.route('/get_date_time', methods=['POST'])
def get_date_time():
    request_parameter = request.get_json()
    date_time_string = request_parameter.get("date_time")
    print(type(date_time_string))
    print(f"date and time as string: {date_time_string}")
    date_time = datetime.fromisoformat(date_time_string)
    print(type(date_time))
    print(f"date and time as datetime: {date_time}")
    return Response(status=200)


@app.route('/get_records_with_limit_and_offset', methods=['GET'])
def get_records_with_limit_and_offset():
    database_name = request.args.get("database_name")
    table_name = request.args.get("table_name")
    limit = request.args.get("limit")
    offset = request.args.get("offset")
    result = database_handler.get_records_with_limit_and_offset(database_name, table_name, limit, offset)
    return jsonify(result)


@app.route('/schemas', methods=['GET'])
def get_schemas():
    schemas = database_handler.get_schemas()
    return jsonify(schemas)


@app.route('/tables', methods=['GET'])
def get_tables():
    database_name = request.args.get("database_name")
    tables = database_handler.get_tables_of_given_database(database_name)
    return jsonify(tables)


@app.route('/row_and_column', methods=['GET'])
def get_rows_and_colums():
    schema_name = request.args.get("schema_name")
    table_name = request.args.get("table_name")
    detailed_information_about_table = database_handler.get_detailed_information_about_table(schema_name=schema_name,
                                                                                             table_name=table_name)
    return jsonify(detailed_information_about_table)


@app.route('/available_cpu_core', methods=['GET'])
def get_available_cpu_core():
    core_number = os.cpu_count()
    return jsonify(core_number)


@app.route('/metrics', methods=['GET'])
def get_metrics():
    estimator_id = request.args.get("estimator_id")
    if estimator_id is None:
        result = database_handler.get_all_metrics()
    else:
        result = database_handler.get_metrics_by_estimator_id(estimator_id)
    return jsonify(result)


@app.route('/fit', methods=['POST'])
def fit():
    logging.info('fit POST request')
    response = None
    request_parameter = request.get_json()
    train_task_id = request_parameter.get("train_task_id")
    used_cpu_core = request_parameter.get("used_cpu_core")
    available_feature_selectors = get_feature_selectors(used_cpu_core)
    available_samplers = get_samplers(used_cpu_core)
    available_scalers = get_scalers()
    available_models = get_models(used_cpu_core)
    is_train_parameters_are_proper = True
    if train_task_id is None:
        logger.error("train_task_id not set")
        is_train_parameters_are_proper = False
    else:
        # database_handler = database.Handler(log_file, log_level, database_url, database_user, database_password)
        train_parameters = database_handler.get_train_parameters(train_task_id)
        planned_encoding_id = train_parameters[1]
        if planned_encoding_id is None:
            logger.error("Planned encoding id not set")
            is_train_parameters_are_proper = False

        planned_feature_engineering_id = train_parameters[2]
        if planned_feature_engineering_id is None:
            logger.info("Feature engineering off")

        feature_selector_key = train_parameters[3]
        if feature_selector_key is None:
            logger.error("Feature selector not set")
            is_train_parameters_are_proper = False
        else:
            feature_selector_name = available_feature_selectors.get(int(feature_selector_key))[0]
            feature_selector = available_feature_selectors.get(int(feature_selector_key))[1]
            logger.info(f"Feature selector: {feature_selector_name}")
            if feature_selector is None:
                logger.error("Feature selector doesn't exist in train application")
                is_train_parameters_are_proper = False

        sampler_key = train_parameters[4]
        if sampler_key is None:
            logger.error("Sampler not set")
            is_train_parameters_are_proper = False
        else:
            sampler_name = available_samplers.get(int(sampler_key))[0]
            sampler = available_samplers.get(int(sampler_key))[1]
            logger.info(f"Sampler: {sampler_name}")
            if sampler is None:
                logger.error("Sampler doesn't exist in train application")
                is_train_parameters_are_proper = False

        scaler_key = train_parameters[5]
        if scaler_key is None:
            logger.error("Scaler not set")
            is_train_parameters_are_proper = False
        else:
            scaler_name = available_scalers.get(int(scaler_key))[0]
            scaler = available_scalers.get(int(scaler_key))[1]
            logger.info(f"Scaler: {scaler_name}")
            if scaler is None:
                logger.error("Scaler doesn't exist in train application")
                is_train_parameters_are_proper = False

        model_key = train_parameters[6]
        if model_key is None:
            logger.error("Model not set")
            is_train_parameters_are_proper = False
        else:
            model_name = available_models.get(int(model_key))[0]
            model = available_models.get(int(model_key))[1]
            logger.info(f"Model: {model_name}")
            if model is None:
                logger.error("Model doesn't exist in train application")
                is_train_parameters_are_proper = False

        split_test_size = None
        saved_test_split_size = train_parameters[6]
        if saved_test_split_size is None:
            logger.info("No test split size set, use default value, 0.25")
        else:
            split_test_size = saved_test_split_size
            logger.info(f"Train test split size: {split_test_size}")

        if is_train_parameters_are_proper:
            fit_process_parameter = dict()
            fit_process_parameter["train_task_id"] = train_task_id
            fit_process_parameter["planned_encoding_id"] = planned_encoding_id
            fit_process_parameter["planned_feature_engineering_id"] = planned_feature_engineering_id
            fit_process_parameter["feature_selector"] = feature_selector
            fit_process_parameter["sampler"] = sampler
            fit_process_parameter["scaler"] = scaler
            fit_process_parameter["model"] = model
            fit_process_parameter["used_cpu_core"] = used_cpu_core
            fit_process_parameter["split_test_size"] = split_test_size
            thread = Thread(target=fit, args=(fit_process_parameter,))
            thread.start()
            logger.info(f"Train parameters are proper, fit process started, train_task_id: {train_task_id}")
            response = Response(status=200)
        else:
            logger.info(f"Train parameters are not proper, fit process didn't start, train_task_id: {train_task_id}")
            response = Response(status=400)
    return response


def fit(parameter):
    logger.info("Fit process begin")
    dataset_to_train = None
    message = dict()
    train_task_id = parameter.get("train_task_id")
    planned_encoding_id = parameter.get('planned_encoding_id')
    planned_feature_engineering_id = parameter.get("planned_feature_engineering_id")
    feature_selector = parameter.get("feature_selector")
    sampler = parameter.get("sampler")
    scaler = parameter.get("scaler")
    model = parameter.get("model")
    split_test_size = parameter.get("split_test_size")
    used_cpu_core = parameter.get("used_cpu_core")

    logger.info("database_encoding_begin")
    message["method"] = "DATABASE_ENCODING_BEGIN"
    message["train_task_id"] = train_task_id
    send_message(message)
    start_encode = time.time()
    encoded_table_registry_id, encoding_parameters, encoded_fields, encoder_object_by_field = database_handler.encode(
        planned_encoding_id)
    finish_encode = time.time()
    encode_processing_time = finish_encode - start_encode
    logger.info(f"Encode processing time: {encode_processing_time} sec")
    logger.info("database_encoding_finished")
    message["method"] = "DATABASE_ENCODING_FINISHED"
    send_message(message)

    schema_name, encoded_table_name = database_handler.get_encoded_table_name(encoded_table_registry_id)
    encoded_dataset = database_handler.get_all_records_from_database(schema_name, encoded_table_name)
    feature_engineering_parameters = None
    if planned_feature_engineering_id is None:
        dataset_to_train = encoded_dataset
    else:
        message["method"] = "FEATURE_ENGINEERING_BEGIN"
        message["train_task_id"] = train_task_id
        start_feature_engineering = time.time()
        send_message(message)
        dataset_to_train, feature_engineering_parameters, feature_engineered_table_registry_id = database_handler.do_feature_engineering(
            schema_name,
            encoded_table_name,
            encoded_dataset,
            encoded_fields,
            planned_feature_engineering_id,
            used_cpu_core)
        finish_feature_engineering = time.time()
        feature_engineering_processing_time = finish_feature_engineering - start_feature_engineering
        logger.info(f"Feature engineering finished, processing time: {feature_engineering_processing_time} sec")
        message["method"] = "FEATURE_ENGINEERING_FINISHED"
        send_message(message)

    features = dataset_to_train[:, 1:-1]
    labels = dataset_to_train[:, -1:]
    labels = labels.astype(int)
    labels = column_or_1d(labels)
    # sampledFeatures, sampledLabels = sampler.fit_resample(features, labels)
    train_features, test_features, train_labels, test_labels = train_test_split(features, labels,
                                                                                test_size=split_test_size,
                                                                                random_state=0)

    logger.info("sampling_begin")
    message["method"] = "SAMPLING_BEGIN"
    message["train_task_id"] = train_task_id
    send_message(message)
    start_sampling = time.time()
    sampled_train_features, sampled_train_labels = sampler.fit_resample(train_features, train_labels)
    finish_sampling = time.time()
    sampling_proessing_time = finish_sampling - start_sampling
    logger.info(f"Sampling finished, processing time: {sampling_proessing_time} sec")
    message["method"] = "SAMPLING_FINISHED"
    message["train_task_id"] = train_task_id
    send_message(message)
    pipeline = Pipeline(
        [('scaler', scaler), ('featureSelector', feature_selector), ('model', model)]
    )
    logger.info("pipeline_fit_begin")
    message["method"] = "PIPELINE_FIT_BEGIN"
    send_message(message)
    start_fit = time.time()
    pipeline.fit(sampled_train_features, sampled_train_labels)
    finish_fit = time.time()
    fit_processing_time = finish_fit - start_fit
    logger.info(f"Pipeline fit finished, processing time: {fit_processing_time} sec")
    message["method"] = "PIPELINE_FITTING_FINISHED"
    send_message(message)
    predicted_labels = pipeline.predict(test_features)
    conf_matrix = confusion_matrix(test_labels, predicted_labels)
    logger.info(f"Confusion Matrix: {conf_matrix}, train_task_id: {train_task_id}")
    estimator = dict()

    # New begin
    estimator["pipeline"] = pipeline
    estimator["encoder_object_by_field"] = encoder_object_by_field
    estimator["encoding_parameters"] = encoding_parameters
    estimator["feature_engineering_parameters"] = feature_engineering_parameters
    estimator["feature_engineered_table_registry_id"] = feature_engineered_table_registry_id
    # New endd
    estimator_id = database_handler.persist_estimator(train_task_id, estimator)
    calculate_and_save_metrics(estimator_id, conf_matrix, test_labels, predicted_labels)
    message["method"] = "PIPELINE_PERSISTED"
    send_message(message)


def calculate_and_save_metrics(estimator_id, conf_matrix, test_labels, predicted_labels):
    TP = int(conf_matrix[0][0])
    FP = int(conf_matrix[0][1])
    FN = int(conf_matrix[1][0])
    TN = int(conf_matrix[1][1])
    temp = TP + FN
    sensitivity = 0
    if temp != 0:
        sensitivity = TP / (TP + FN)
    temp = TN + FP
    specificity = 0
    if temp != 0:
        specificity = TN / (TN + FP)
    accuracy = accuracy_score(test_labels, predicted_labels)
    balanced_accuracy = balanced_accuracy_score(test_labels, predicted_labels)
    precision = 0
    temp = TP + FP
    if temp != 0:
        precision = TP / (TP + FP)
    recall = recall_score(test_labels, predicted_labels)
    temp = TP + FN
    PPV = 0
    if temp != 0:
        PPV = TP / (TP + FN)
    temp = TN + FN
    NPV = 0
    if temp != 0:
        NPV = TN / (TN + FN)
    temp = FN + TP
    FNR = 0
    if temp != 0:
        FNR = FN / (FN + TP)
    temp = FP + TN
    FPR = 0
    if temp != 0:
        FPR = FP / (FP + TN)
    FDR = 0
    temp = FP + TP
    if temp != 0:
        FDR = FP / (FP + TP)
    temp = FN + TN
    FOR = 0
    if temp != 0:
        FOR = FN / (FN + TN)
    f1 = f1_score(test_labels, predicted_labels)
    f_05 = calculateF(0.5, precision, recall)
    f2 = calculateF(2, precision, recall)
    temp = math.sqrt(TP + FP) * math.sqrt(TP + FN) * math.sqrt(TN + FP) * math.sqrt(TN + FN)
    MCC = 0
    if temp != 0:
        MCC = (TP * TN - FP * FN) / temp
    ROCAUC = roc_auc_score(test_labels, predicted_labels)
    Youdens_statistic = sensitivity + specificity - 1
    database_handler.persist_metrics(estimator_id, TP, FP, TN, FN, sensitivity, specificity, accuracy,
                                     balanced_accuracy,
                                     precision, recall, PPV, NPV, FNR, FPR, FDR, FOR, f1, f_05, f2, MCC, ROCAUC,
                                     Youdens_statistic)


def calculateF(beta, precision, recall):
    temp = beta * beta * precision + recall
    if temp != 0:
        f_beta = (1 + beta) * (1 + beta) * precision * recall / temp
    else:
        f_beta = 0
    return f_beta


# @server.route('/message')
# def hello_world():
#     d = dict()
#     d["from"] = "Tom"
#     d["entry"] = "Hello World"
#     response = jsonify(d)
#     send_message(d)
#     return response


if __name__ == "__main__":
    log_file = os.getenv("LOGFILE", "C:/Temp/train.log")
    log_level = os.getenv("LOG_LEVEL", "DEBUG")
    rabbitmq_url = os.getenv("RABBITMQ_URL", "localhost")
    database_url = os.getenv("MYSQL_DATABASE_URL", "localhost")
    database_user = os.getenv("MYSQL_USER", "root")
    database_password = os.getenv("MYSQL_PASSWORD", "TOmi1970")
    file_handler = logging.FileHandler(log_file)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    logger = logging.getLogger("train.server")
    logger.setLevel(log_level)
    logger.addHandler(file_handler)
    logger.info("RabbitMQ: " + rabbitmq_url)
    logger.info("MYSQL_DATABASE_URL: " + database_url)
    logger.info("MYSQL_USER :" + database_user)
    logger.info("MYSQL_PASSWORD: " + database_password)
    logger.info("Train application started")
    database_handler = database_module.Handler(database_url, database_user, database_password)
    database_handler.create_common_fraud_schemas()

    # databaseHandler = database.DatabaseHandler(logging, database_url, database_user, database_password)
    # encoded_records, encoders = databaseHandler.get_encoded_records_and_encoders("card_10000_5")
    # print(encoded_records)
    # print(encoders)
    app.run(host='0.0.0.0', port=8085, debug=True)
