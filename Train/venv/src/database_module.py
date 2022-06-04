import json

import sys
import time

import mysql.connector
import numpy as np
import logging
import pickle

import encoding_module
import engineering_module


class IllegalArgumentException(Exception):
    pass

class NoneException(Exception):
    pass


class NumpyMySQLConverter(mysql.connector.conversion.MySQLConverter):
    """ A mysql.connector Converter that handles Numpy types """

    def _float32_to_mysql(self, value):
        return float(value)

    def _float64_to_mysql(self, value):
        return float(value)

    def _int32_to_mysql(self, value):
        return int(value)

    def _int64_to_mysql(self, value):
        return int(value)


class Handler:
    def __init__(self, database_url, database_user, database_password):
        self.logger = logging.getLogger("train.server.database")
        self.database_url = database_url
        self.database_user = database_user
        self.database_password = database_password
        self.connection = self.get_connection(self.database_url, self.database_user, self.database_password)

    def get_connection(self, database_url, database_user_name, database_password):
        connection = None
        try:
            connection = mysql.connector.connect(
                # pool_name="local",
                # pool_size=16,
                host=database_url,
                user=database_user_name,
                password=database_password)
            self.logger.info("Database connection created")
        except mysql.connector.Error as err:
            self.logger.error(f"MySQL connection error: {err.msg}")
        # connection.set_converter_class(NumpyMySQLConverter)
        return connection

    def create_common_fraud_schemas(self):
        cursor = self.connection.cursor()
        commands = ["SQL CREATE SCHEMA common_fraud.txt", "SQL CREATE TABLE planned_encoding.txt",
                    "SQL CREATE TABLE encoded_table_registry.txt",
                    "SQL CREATE TABLE planned_encoding_and_encoded_table_version.txt", "SQL CREATE TABLE encoder.txt",
                    "SQL CREATE TABLE train_task.txt", "SQL CREATE TABLE estimator.txt", "SQL CREATE TABlE metrics.txt",
                    "SQL CREATE TABLE planned_feature_engineering.txt",
                    "SQL CREATE TABLE feature_engineered_table_registry.txt",
                    "SQL CREATE TABLE planned_and_created_feature_engineering_table_version.txt"]
        for command in commands:
            try:
                file = open(command, "r")
                sql_create_script = file.read()
            except FileNotFoundError:
                self.logger.error(f"SQL script file not found {command}")
            except OSError:
                self.logger.error("OS Error")
            try:
                cursor.execute(sql_create_script)
                self.connection.commit()
            except mysql.connector.Error as err:
                self.logger.error(f"MySQL error message: {err.msg}")
        self.logger.info("common fraud tables created")
        cursor.close()

    def encode(self, planned_encoding_id):
        encoded_table_registry_id = self.get_encoded_table_registry_id_by_planned_encoding_id(planned_encoding_id)
        if encoded_table_registry_id is None:
            encoding_properties = self.get_encoding_plan_by_id(planned_encoding_id)
            schema_name = encoding_properties.get("schema_name")
            table_name = encoding_properties.get("table_name")
            detailed_information_about_table = self.get_detailed_information_about_table(schema_name, table_name)
            encoding_parameters = encoding_properties.get("encoding_parameters")
            original_dataset = self.get_all_records_from_database(schema_name, table_name)
            original_fields = self.get_field_names_of_table(schema_name, table_name)
            database_encoder = encoding_module.DataBaseEncoder(original_dataset, original_fields)
            encoded_array, encoder_by_field_name, modified_fields_after_encoding = database_encoder.encode(
                encoding_parameters, detailed_information_about_table)
            self.persist_encoded_table_and_encoders(schema_name, table_name, encoded_array, encoder_by_field_name,
                                                    modified_fields_after_encoding, planned_encoding_id)

        encoded_table_registry_id = self.get_encoded_table_registry_id_by_planned_encoding_id(planned_encoding_id)
        encoding_parameters = self.get_encoding_plan_by_id(planned_encoding_id).get("encoding_parameters")
        encoder_object_by_field = self.get_encoder_object_by_field_based_on_planned_encoding_id(planned_encoding_id)
        encoded_fields = self.get_encoded_fields_by_encoded_table_registry_id(encoded_table_registry_id)
        return encoded_table_registry_id, encoding_parameters, encoded_fields, encoder_object_by_field

    def get_encoded_table_registry_id_by_planned_encoding_id(self, planned_encoding_id):
        cursor = self.connection.cursor()
        sql_select_query = "SELECT encoded_table_registry_id FROM common_fraud.planned_encoding_and_encoded_table_version WHERE planned_encoding_id = %s"
        parameter = (planned_encoding_id,)
        try:
            cursor.execute(sql_select_query, parameter)
            result = cursor.fetchone()
            cursor.close()
        except mysql.connector.Error as err:
            self.logger.error(f"MySQL error message: {err.msg}")
        return result

    def get_encoder_object_by_field_based_on_planned_encoding_id(self, planned_encoding_id):
        cursor = self.connection.cursor()
        sql_select_query = "SELECT schema_name, table_name FROM common_fraud.planned_encoding WHERE planned_encoding_id = %s"
        parameter = (planned_encoding_id,)
        try:
            cursor.execute(sql_select_query, parameter)
            result = cursor.fetchone()
        except mysql.connector.Error as err:
            self.logger.error(f"MySQL error message: {err.msg}")
        schema_name = result[0]
        table_name = result[1]
        sql_select_query = "SELECT field_name, encoder_object FROM common_fraud.encoder WHERE schema_name = %s and  table_name = %s"
        parameter = (schema_name, table_name)
        try:
            cursor.execute(sql_select_query, parameter)
            query_result = cursor.fetchall()
            cursor.close()
        except mysql.connector.Error as err:
            self.logger.error(f"MySQL error message: {err.msg}")
        result = dict()
        for field_name, encoder_object in query_result:
            result[field_name] = encoder_object
        return result

    def get_encoded_fields_by_encoded_table_registry_id(self, encoded_table_registry_id):
        cursor = self.connection.cursor()
        sql_select_query = "SELECT schema_name, encoded_table_name FROM common_fraud.encoded_table_registry WHERE id = %s"
        parameter = (encoded_table_registry_id,)
        try:
            cursor.execute(sql_select_query, parameter)
            query_result = cursor.fetchone()
        except mysql.connector.Error as err:
            self.logger.error(f"MySQL error message: {err.msg}")
        encoded_table_name = query_result[0]
        sql_select_query = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s ORDER BY ordinal_position"
        parameter = (encoded_table_name,)
        try:
            cursor.execute(sql_select_query, parameter)
            query_result = cursor.fetchone()
            cursor.close()
        except mysql.connector.Error as err:
            self.logger.error(f"MySQL error message: {err.msg}")
        result = list()
        field_num = len(query_result)
        for i in range(field_num):
            if i != 0 or i != field_num - 1:  # id és fraud mezőre nics szükség
                result.append(field_num[i])
        return result

    def persist_encoded_table_and_encoders(self, schema_name, table_name, encoded_array, encoder_by_field_name,
                                           modified_fields_after_encoding, planned_encoding_id):
        encoded_table_name = self.create_and_registry_encoded_table(schema_name, table_name,
                                                                    modified_fields_after_encoding, planned_encoding_id)
        self.persist_encoded_table(schema_name, encoded_table_name, modified_fields_after_encoding, encoded_array)
        for field_name, encoder in encoder_by_field_name:
            self.persist_encoder(schema_name, table_name, field_name, encoder)

    def create_and_registry_encoded_table(self, schema_name, original_table_name, reduced_field_list,
                                          planned_encoding_id):
        cursor = self.connection.cursor()
        version = None
        # Létezik már az adott sémájú és tábla nevű kódol adatbázis az encoded_table_registry táblában
        sql_select_query = f"SELECT version FROM common_fraud.encoded_table_registry WHERE schema_name = {schema_name} and table_name = {original_table_name}"
        try:
            cursor.execute(sql_select_query)
            version = cursor.fetchone()
            cursor.close()
        except mysql.connector.Error as err:
            self.logger.error(f"MySQL error message: {err.msg}")
        if version is None:
            version = 1
        else:
            version = version + 1
        encoded_table_name = f"encoded_{original_table_name}" + version
        # Kódolt tábla létrehozása
        sql_create_script = f"CREATE TABLE IF NOT EXISTS {schema_name}.{encoded_table_name} (id BIGINT NOT NULL AUTO_INCREMENT, "
        for field in reduced_field_list:
            sql_create_script = sql_create_script + f"{field} DOUBLE, "
        sql_create_script = sql_create_script + " PRIMARY KEY (id)) engine = InnoDB"
        try:
            cursor.execute(sql_create_script)
            self.connection.commit()
        except mysql.connector.Error as err:
            self.logger.error(f"MySQL error message: {err.msg}")
        self.logger.info(f"Encoded table created, schema: {schema_name}, table name: {encoded_table_name}")
        # Beszúrás encoded_table_registry-be
        sql_insert_command = "INSERT INTO  common_fraud.encoded_table_registry (schema_name, table_name, version, encoded_table_name) VALUES(%s, %s, %s, %s)"
        parameter = (schema_name, original_table_name, version, encoded_table_name)
        try:
            cursor.execute(sql_insert_command, parameter)
            encoded_table_registry_last_inserted_row_id = cursor.lastrowid
            self.connection.commit()
            self.logger.info(
                f"Encoded schema created, schema name: {schema_name}, original table name: {original_table_name}, version: {version}, encoded table name: {encoded_table_name}")
        except mysql.connector.Error as err:
            self.logger.error(f"MySQL error message: {err.msg}")
        # Beszúrás planned_encoding_and_encoded_table_version táblába
        sql_insert_command = "INSERT INTO  common_fraud.planned_encoding_and_encoded_table_version (planned_encoding_id, encoded_table_registry_id) VALUES(%s, %s)"
        parameter = (planned_encoding_id, encoded_table_registry_last_inserted_row_id)
        try:
            cursor.execute(sql_insert_command, parameter)
            self.connection.commit()
            self.logger.info(
                f"planned_encoding_and_encoded_table_version junction table updated,planned_encoding_id: {planned_encoding_id}, encoded_table_registry_id: {encoded_table_registry_last_inserted_row_id}")
        except mysql.connector.Error as err:
            self.logger.error(f"MySQL error message: {err.msg}")
        cursor.close()
        return encoded_table_name

    def persist_encoded_table(self, schema_name, encoded_table_name, field_list, values_array):
        cursor = self.connection.cursor()
        sql_insert_query = f"INSERT INTO {schema_name}.{encoded_table_name} ("
        length_of_fields = len(field_list)
        for i in length_of_fields:
            field = field_list[i]
            sql_insert_query = sql_insert_query + f"{field}"
            if i != length_of_fields - 1:
                sql_insert_query = sql_insert_query + ","
        sql_insert_query = sql_insert_query + ") VALUES ("
        for i in length_of_fields:
            sql_insert_query = sql_insert_query + "%s"
            if i != length_of_fields - 1:
                sql_insert_query = sql_insert_query + ","
        sql_insert_query = sql_insert_query + ")"
        length_of_dataset = len(values_array)
        bound = 1000
        if length_of_dataset > bound:
            number_of_part_array = int(length_of_dataset / bound)
            number_of_rest_datas = length_of_dataset - number_of_part_array * bound
            for i in range(0, number_of_part_array, 1):
                temp_array = values_array[i * bound:(i + 1) * bound, :]
                value_list = list()
                for record in temp_array:
                    value_list.append(tuple(record))
                cursor.executemany(sql_insert_query, value_list)
                self.connection.commit()
            temp_array = values_array[
                         (number_of_part_array) * bound:(number_of_part_array) * bound + number_of_rest_datas, :]
            value_list = list()
            for record in temp_array:
                value_list.append(tuple(record))
            cursor.executemany(sql_insert_query, value_list)
            self.connection.commit()
        else:
            value_list = list()
            for record in values_array:
                value_list.append(tuple(record))
            cursor.executemany(sql_insert_query, value_list)
            self.connection.commit()
        cursor.close()

    def persist_train_task(self, request_parameter, feature_selector_keys, samplers_keys, scalers_keys, models_keys):
        cursor = self.connection.cursor()
        planned_encoding_id = request_parameter.get("planned_encoding_id")
        if planned_encoding_id is None:
            raise IllegalArgumentException(
                {"message": "Parameter is missing in request body", "request_parameter": "planned_encoding_id"})
        existing_planned_encoding_id = self.get_existing_planned_encoding_id()
        if planned_encoding_id not in existing_planned_encoding_id:
            raise IllegalArgumentException(
                {"message": "Parameter is invalid", "request_parameter": "planned_encoding_id"})
        planned_feature_engineering_id = request_parameter.get("planned_feature_engineering_id")
        existing_planned_feature_engineering_id = self.get_existing_planned_feature_engineering_id()
        if planned_feature_engineering_id not in existing_planned_feature_engineering_id and planned_feature_engineering_id is not None:
            raise IllegalArgumentException(
                {"message": "Parameter is invalid", "request_parameter": "planned_feature_engineering_id"})
        feature_selector_code = request_parameter.get("feature_selector_code")
        if feature_selector_code is None:
            raise IllegalArgumentException(
                {"message": "Parameter is missing in request body", "request_parameter": "feature_selector_code"})
        if feature_selector_code not in feature_selector_keys:
            raise IllegalArgumentException(
                {"message": "Parameter is invalid", "request_parameter": "feature_selector_code"})
        sampler_code = request_parameter.get("sampler_code")
        if sampler_code is None:
            raise IllegalArgumentException(
                {"message": "Parameter is missing in request body", "request_parameter": "sampler_code"})
        if sampler_code not in samplers_keys:
            raise IllegalArgumentException({"message": "Parameter is invalid", "request_parameter": "sampler_code"})
        scaler_code = request_parameter.get("scaler_code")
        if scaler_code is None:
            raise IllegalArgumentException(
                {"message": "Parameter is missing in request body", "request_parameter": "scaler_code"})
        if scaler_code not in scalers_keys:
            raise IllegalArgumentException({"message": "Parameter is invalid", "request_parameter": "scaler_code"})
        model_code = request_parameter.get("model_code")
        if model_code is None:
            raise IllegalArgumentException(
                {"message": "Parameter is missing in request body", "request_parameter": "model_code"})
        if model_code not in models_keys:
            raise IllegalArgumentException({"message": "Parameter is invalid", "request_parameter": "model_code"})
        test_size = request_parameter.get("test_size")
        if test_size is None:
            raise IllegalArgumentException(
                {"message": "Parameter is missing in request body", "request_parameter": "test_size"})
        if test_size > 1 or test_size < 0:
            raise IllegalArgumentException({"message": "Parameter is invalid", "request_parameter": "test_size"})

        sql_insert_query = "INSERT INTO common_fraud.train_task (planned_encoding_id, planned_feature_engineering_id, feature_selector_code, sampler_code, scaler_code, model_code, test_size) VALUES (%s,%s,%s,%s,%s,%s,%s)"
        parameter = (
            planned_encoding_id, planned_feature_engineering_id, feature_selector_code, sampler_code, scaler_code,
            model_code, test_size)
        try:
            cursor.execute(sql_insert_query, parameter)
            last_inserted_row_id = cursor.lastrowid
            self.connection.commit()
            self.logger.info(
                f"Train task persisted, planned_encoding_id: {planned_encoding_id}, planned_feature_engineering_id: {planned_feature_engineering_id}, feature_selector_code: {feature_selector_code}, sampler_code: {sampler_code}, scaler_code: {scaler_code}, model_code: {model_code}, test_size: {test_size}")
        except mysql.connector.Error as err:
            self.logger.error(f"MySQL error message: {err.msg}")
        cursor.close()
        return last_inserted_row_id

    def get_all_train_task(self):
        cursor = self.connection.cursor()
        result = list()
        sql_select_query = "SELECT * FROM common_fraud.train_task"
        try:
            cursor.execute(sql_select_query)
            query_result = cursor.fetchall()
            cursor.close()
        except mysql.connector.Error as err:
            self.logger.error(f"MySQL error message: {err.msg}")
        for item in query_result:
            result.append(item[0])
        return result

    def get_train_task_by_id(self, id):
        cursor = self.connection.cursor()
        result = list()
        sql_select_query = "SELECT * FROM common_fraud.train_task WHERE id = %s"
        parameter = (id,)
        try:
            cursor.execute(sql_select_query, parameter)
            query_result = cursor.fetchall()
            cursor.close()
        except mysql.connector.Error as err:
            self.logger.error(f"MySQL error message: {err.msg}")
        for item in query_result:
            result.append(item[0])
        return result

    def get_existing_planned_encoding_id(self):
        cursor = self.connection.cursor()
        result = list()
        sql_select_query = "select id from common_fraud.planned_encoding"
        try:
            cursor.execute(sql_select_query)
            query_result = cursor.fetchall()
            cursor.close()
        except mysql.connector.Error as err:
            self.logger.error(f"MySQL error message: {err.msg}")
        for item in query_result:
            result.append(item[0])
        return result

    def get_existing_planned_feature_engineering_id(self):
        cursor = self.connection.cursor()
        result = list()
        sql_select_query = "select id from common_fraud.planned_feature_engineering"
        try:
            cursor.execute(sql_select_query)
            query_result = cursor.fetchall()
            cursor.close()
        except mysql.connector.Error as err:
            self.logger.error(f"MySQL error message: {err.msg}")
        for item in query_result:
            result.append(item[0])
        return result

    # train_task műveletek

    # OK
    def get_train_parameters(self, train_task_id):
        cursor = self.connection.cursor()
        self.logger.info("Get train parameters")
        sql_select_query = "select * from common_fraud.train_task where id = %s"
        parameter = (train_task_id,)
        try:
            cursor.execute(sql_select_query, parameter)
            result = cursor.fetchone()
            cursor.close()
        except mysql.connector.Error as err:
            self.logger.error(f"MySQL error message: {err.msg}")
        return result

    def save_feature_engineered_dataset(self, database_name, extended_dataset):
        cursor = self.connection.cursor()
        try:
            cursor.execute("USE " + database_name)
        except mysql.connector.Error as err:
            self.logger.error(f"MySQL error message: {err.msg}")
        try:
            file = open("SQL INSERT feature_engineered_transaction.txt", "r")
            sql_insert_script = file.read()
        except FileNotFoundError:
            self.logger.error("SQL script file not found, unable to insert into feature_engineered transaction table")
        except OSError:
            self.logger.error("OS Error")
        values_array = np.array(extended_dataset)
        self.logger.info(f"feature engineered array shape: {values_array.shape}")
        bound = 1000
        length = len(extended_dataset)
        if length > bound:
            numberOfPartArray = int(length / bound)
            numberOfRestDatas = length - numberOfPartArray * bound
            for i in range(0, numberOfPartArray, 1):
                tempArray = values_array[i * bound:(i + 1) * bound, :]
                valueList = list()
                for record in tempArray:
                    valueList.append(tuple(record))
                try:
                    cursor.executemany(sql_insert_script, valueList)
                    self.connection.commit()
                except mysql.connector.Error as err:
                    self.logger.error(f"MySQL error message: {err.msg}")
            tempArray = values_array[(numberOfPartArray) * bound:(numberOfPartArray) * bound + numberOfRestDatas, :]
            valueList = list()
            for record in tempArray:
                valueList.append(tuple(record))
            try:
                cursor.executemany(sql_insert_script, valueList)
                self.connection.commit()
            except mysql.connector.Error as err:
                self.logger.error(f"MySQL error message: {err.msg}")
        else:
            valueList = list()
            for record in values_array:
                valueList.append(tuple(record))
            try:
                cursor.executemany(sql_insert_script, valueList)
                self.connection.commit()
            except mysql.connector.Error as err:
                self.logger.error(f"MySQL error message: {err.msg}")
        cursor.close()

    def persist_estimator(self, train_task_id, estimator_container):
        cursor = self.connection.cursor()
        sql_insert_query = "INSERT INTO common_fraud.estimator (train_task_id, estimator_object) VALUES (%s,%s)"
        pickled_estimator_container = pickle.dumps(estimator_container)
        parameter = (train_task_id, pickled_estimator_container)
        try:
            cursor.execute(sql_insert_query, parameter)
            last_inserted_row_id = cursor.lastrowid
            self.connection.commit()
            self.logger.info(f"Estimator persisted in database, train_task_id: {train_task_id}")
        except mysql.connector.Error as err:
            self.logger.error(f"MySQL error message: {err.msg}")
        cursor.close()
        return last_inserted_row_id

    def persist_encoding_plan(self, schema_name, table_name, encoding_parameters):
        cursor = self.connection.cursor()
        sql_insert_query = "INSERT INTO common_fraud.planned_encoding (schema_name, table_name, encoding_parameters) VALUES (%s,%s,%s)"
        parameter = (schema_name, table_name, json.dumps(encoding_parameters))
        try:
            cursor.execute(sql_insert_query, parameter)
            last_inserted_row_id = cursor.lastrowid
            self.connection.commit()
            self.logger.info(
                f"Encoding plan persisted, schema name: {schema_name}, table name: {table_name}, encoding parameters: {encoding_parameters}")
        except mysql.connector.Error as err:
            self.logger.error(f"MySQL error message: {err.msg}")
        cursor.close()
        return last_inserted_row_id

    def get_all_encoding_plan(self):
        response = list()
        cursor = self.connection.cursor()
        sql_select_query = "SELECT * FROM common_fraud.planned_encoding"
        try:
            cursor.execute(sql_select_query)
            query_result = cursor.fetchall()
            cursor.close()
        except mysql.connector.Error as err:
            self.logger.error(f"MySQL error message: {err.msg}")
        for record in query_result:
            single_response = dict()
            single_response["id"] = record[0]
            single_response["schema_name"] = record[1]
            single_response["table_name"] = record[2]
            single_response["encoding_parameters"] = json.loads(record[3])
            response.append(single_response)
        return response

    def get_encoding_plan_by_id(self, id):
        cursor = self.connection.cursor()
        sql_select_query = f"SELECT * FROM common_fraud.planned_encoding WHERE id={id}"
        try:
            cursor.execute(sql_select_query)
            query_result = cursor.fetchone()
            cursor.close()
        except mysql.connector.Error as err:
            self.logger.error(f"MySQL error message: {err.msg}")
        if query_result is None:
            raise NoneException({"message": "The given id doesn't exist", "parameter": id})
        response = dict()
        response["schema_name"] = query_result[1]
        response["table_name"] = query_result[2]
        response["encoding_parameters"] = json.loads(query_result[3])
        return response

    # OK
    def get_encoded_table_name_by_id(self, encoded_table_registry_id):
        cursor = self.connection.cursor()
        sql_select_query = f"SELECT schema_name, encoded_table_name FROM common_fraud.encoded_table_registry WHERE id = {encoded_table_registry_id}"
        try:
            cursor.execute(sql_select_query)
            result = cursor.fetchall()
            cursor.close()
        except mysql.connector.Error as err:
            self.logger.error(f"MySQL error message: {err.msg}")
        return result

    def persist_feature_engineering_plan(self, schema_name, table_name, feature_engineering_parameters):
        cursor = self.connection.cursor()
        sql_insert_query = "INSERT INTO common_fraud.planned_feature_engineering (schema_name, table_name, feature_engineering_parameters) VALUES (%s,%s,%s)"
        parameter = (schema_name, table_name, json.dumps(feature_engineering_parameters))
        try:
            cursor.execute(sql_insert_query, parameter)
            last_inserted_row_id = cursor.lastrowid
            self.connection.commit()
            self.logger.info(
                f"Feature engineering plan persisted, schema name: {schema_name}, table name: {table_name}, feature engineering parameters: {feature_engineering_parameters}")
        except mysql.connector.Error as err:
            self.logger.error(f"MySQL error message: {err.msg}")
        cursor.close()
        return last_inserted_row_id

    def get_all_feature_engineering_plan(self):
        response = list()
        cursor = self.connection.cursor()
        sql_select_query = "SELECT * FROM common_fraud.planned_feature_engineering"
        try:
            cursor.execute(sql_select_query)
            query_result = cursor.fetchall()
            cursor.close()
        except mysql.connector.Error as err:
            self.logger.error(f"MySQL error message: {err.msg}")
        for record in query_result:
            single_response = dict()
            single_response["id"] = record[0]
            single_response["schema_name"] = record[1]
            single_response["table_name"] = record[2]
            single_response["feature_engineering_parameters"] = json.loads(record[3])
            response.append(single_response)
        return response

    def get_feature_engineering_plan_by_id(self, id):
        cursor = self.connection.cursor()
        sql_select_query = f"SELECT * FROM common_fraud.planned_feature_engineering WHERE id = {id}"
        try:
            cursor.execute(sql_select_query)
            result = cursor.fetchone()
            cursor.close()
        except mysql.connector.Error as err:
            self.logger.error(f"MySQL error message: {err.msg}")
        response = dict()
        response["schema_name"] = result[1]
        response["table_name"] = result[2]
        response["feature_engineering_parameters"] = json.loads(result[3])
        return response

    def get_feature_enginnering_table_registry_id(self, encoded_table_name, planned_feature_engineering_id):
        cursor = self.connection.cursor()
        sql_select_query = f"SELECT feature_engineered_table_registry_id FROM common_fraud.planned_and_created_featured_engineering_table_version WHERE encoded_table_name = {encoded_table_name} and planned_feature_engineering_id = {planned_feature_engineering_id}"
        try:
            cursor.execute(sql_select_query)
            result = cursor.fetchone()
            cursor.close()
        except mysql.connector.Error as err:
            self.logger.error(f"MySQL error message: {err.msg}")
        return result

    def is_encoded_table_exist(self, planned_encoding_id):
        cursor = self.connection.cursor()
        sql_select_query = f"SELECT encoded_table_registry_id FROM common_fraud.planned_encoding_and_encoded_table_version WHERE planned_encoding_id = {planned_encoding_id}"
        try:
            cursor.execute(sql_select_query)
            query_result = cursor.fetchall()
            cursor.close()
        except mysql.connector.Error as err:
            self.logger.error(f"MySQL error message: {err.msg}")
        if query_result is None:
            result = False
        else:
            result = True
        return result

    def persist_encoder(self, schema_name, table_name, field_name, encoder_object):
        cursor = self.connection.cursor()
        sql_insert_query = "INSERT INTO common_fraud.encoder (schema_name,table_name, field_name, encoder_object ) VALUES (%s,%s,%s,%s)"
        pickled_encoder = pickle.dumps(encoder_object)
        parameter = (schema_name, table_name, field_name, pickled_encoder)
        try:
            cursor.execute(sql_insert_query, parameter)
            last_inserted_row_id = cursor.lastrowid
            self.connection.commit()
            self.logger.info(
                f"Encoder persisted, schema name: {schema_name},table name: {table_name}, field name: {field_name}")
        except mysql.connector.Error as err:
            self.logger.error(f"MySQL error message: {err.msg}")
        cursor.close()
        return last_inserted_row_id

    def saveEncoder(self, connection, databaseName):
        cursor = connection.cursor()
        sqlUseQuery = "USE " + databaseName
        cursor.execute(sqlUseQuery)
        sqlInsertQuery = "INSERT INTO encoder (encoder_name,encoder_object) VALUES (%s,%s)"
        pickledCurrencyEncoder = pickle.dumps(self.currencyEncoder)
        cursor.execute(sqlInsertQuery, ("currency_encoder", pickledCurrencyEncoder))
        pickledCountryEncoder = pickle.dumps(self.countryEncoder)
        cursor.execute(sqlInsertQuery, ("country_encoder", pickledCountryEncoder))
        connection.commit()
        cursor.close()

    def persist_encoded_table_name(self, table_name):
        cursor = self.connection.cursor()
        sql_insert_query = "INSERT INTO common_fraud.encoded_table_registry (encoded_table_name) VALUES (%s)"
        parameter = (table_name,)
        try:
            cursor.execute(sql_insert_query, parameter)
            last_inserted_row_id = cursor.lastrowid
            self.connection.commit()
            self.logger.info(f"Encoded table name persisted, table name: {table_name}")
        except mysql.connector.Error as err:
            self.logger.error(f"MySQL error message: {err.msg}")
        cursor.close()
        return last_inserted_row_id

        def persist_into_planned_encoding_and_encoded_table_version(self, planned_encoding_id,
                                                                    encoded_table_registry_id):
            cursor = self.connection.cursor()
            sql_insert_query = "INSERT INTO common_fraud.planned_encodigng (encoded_table_name) VALUES (%s)"
            parameter = (table_name,)
            try:
                cursor.execute(sql_insert_query, parameter)
                last_inserted_row_id = cursor.lastrowid
                self.connection.commit()
                self.logger.info(f"Encoded table name persisted, table name: {table_name}")
            except mysql.connector.Error as err:
                self.logger.error(f"MySQL error message: {err.msg}")
            cursor.close()
            return last_inserted_row_id

    def persist_metrics(self, estimator_id, TP, FP, TN, FN, sensitivity, specificity, accuracy, balanced_accuracy,
                        precision, recall, PPV, NPV, FNR, FPR, FDR, FOR, f1, f_05, f2, MCC, ROCAUC, Youdens_statistic):
        cursor = self.connection.cursor()
        sql_insert_query = "INSERt INTO common_fraud.metrics (estimator_id,TP,FP,TN,FN,sensitivity,specificity,accuracy,balanced_accuracy,prec,recall,PPV,NPV,FNR,FPR,FDR,F_OR,f1,f_05,f2,MCC,ROCAUC,Youdens_statistic) VALUES" \
                           "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        values = (estimator_id, TP, FP, TN, FN, sensitivity, specificity, accuracy, balanced_accuracy,
                  precision, recall, PPV, NPV, FNR, FPR, FDR, FOR, f1, f_05, f2, MCC, ROCAUC, Youdens_statistic)
        try:
            cursor.execute(sql_insert_query, values)
            self.connection.commit()
            self.logger.info(f"Metrics persisted in database, estimator_id: {estimator_id}")
        except mysql.connector.Error as err:
            self.logger.error(f"MySQL error message: {err.msg}")
        cursor.close()

    def get_all_metrics(self):
        cursor = self.connection.cursor()
        sql_select_query = "SELECT * FROM common_fraud.metrics"
        try:
            cursor.execute(sql_select_query)
            query_result = cursor.fetchall()
            cursor.close()
        except mysql.connector.Error as err:
            self.logger.error(f"MySQL error message: {err.msg}")
        complex_response = list()
        for record in query_result:
            single_metrics = self.build_single_metrics_properties(record)
            complex_response.append(single_metrics)
        return complex_response

    def get_metrics_by_estimator_id(self, estimator_id):
        cursor = self.connection.cursor()
        sql_select_query = "SELECT * FROM common_fraud.metrics WHERE estimator_id = %s"
        parameter = (estimator_id, )
        try:
            cursor.execute(sql_select_query, parameter)
            query_result = cursor.fetchone()
            cursor.close()
        except mysql.connector.Error as err:
            self.logger.error(f"MySQL error message: {err.msg}")
        single_response = self.build_single_metrics_properties(query_result)
        return single_response

    def build_single_metrics_properties(self, result_from_query):
        result = dict()
        result["TP"] = result_from_query[2]
        result["FP"] = result_from_query[3]
        result["TN"] = result_from_query[4]
        result["FN"] = result_from_query[5]
        result["sensitivity"] = result_from_query[6]
        result["specificity"] = result_from_query[7]
        result["accuracy"] = result_from_query[8]
        result["balanced_accuracy"] = result_from_query[9]
        result["prec"] = result_from_query[10]
        result["recall"] = result_from_query[11]
        result["recall"] = result_from_query[12]
        result["PPV"] = result_from_query[13]
        result["NPV"] = result_from_query[14]
        result["FNR"] = result_from_query[15]
        result["FPR"] = result_from_query[16]
        result["FDR"] = result_from_query[17]
        result["FOR"] = result_from_query[18]
        result["f1"] = result_from_query[19]
        result["f_05"] = result_from_query[20]
        result["f2"] = result_from_query[21]
        result["MCC"] = result_from_query[22]
        result["ROCAUC"] = result_from_query[23]
        result["Youdens_statistic"] = result_from_query[24]
        return result

    def get_max_version_of_encoded_table(self, schema_name, table_name):
        cursor = self.connection.cursor()
        sql_select_Query = f"SELECT MAX(version) FROM common_fraud.encoded_table_registry WHERE schema_name = {schema_name} AND table_name = {table_name}"
        try:
            cursor.execute(sql_select_Query)
            result = cursor.fetchone()
        except mysql.connector.Error as err:
            self.logger.error(f"MySQL error message: {err.msg}")
        cursor.close()
        self.logger.info(
            f"The last version is: {result} of the encoded table, schema name: {schema_name}, table name: {table_name}")
        return result

    def get_encoded_table_name_by_version(self, schema_name, table_name, version):
        cursor = self.connection.cursor()
        sql_select_Query = f"SELECT encoded_table_name FROM common_fraud.encoded_table_registry WHERE schema_name = {schema_name} AND table_name = {table_name} AND version = {version}"
        try:
            cursor.execute(sql_select_Query)
            result = cursor.fetchone()
        except mysql.connector.Error as err:
            self.logger.error(f"MySQL error message: {err.msg}")
        cursor.close()
        if result is None:
            self.logger.error(
                f"The encoded table doesn't exists, schema name: {schema_name}, table name: {table_name}, version: {version}")
        self.logger.info(
            f"The last version is: {result} of the encoded table, schema name: {schema_name}, table name: {table_name}")
        return result

    def get_all_records_from_database(self, schema_name, table_name):
        start = time.time()
        cursor = self.connection.cursor()
        sql_select_Query = f"select * from {schema_name}.{table_name}  order by timestamp desc"
        try:
            cursor.execute(sql_select_Query)
            result = cursor.fetchall()
            numpy_array = np.array(result)
            end = time.time()
            elapsedTime = end - start
            self.logger.info(
                f'database: {schema_name}, table: {table_name} loaded, loading time: {elapsedTime} sec, record number: {numpy_array.shape}')
        except mysql.connector.Error as err:
            self.logger.error(f"MySQL error message: {err.msg}")
        cursor.close()
        return numpy_array[:, :]

    def get_field_names_of_table(self, schema_name, table_name):
        cursor = self.connection.cursor()
        result = list()
        sql_select_query = f"SELECT COLUMN_NAME, DATA_TYPE  FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '{schema_name}' AND TABLE_NAME = '{table_name}' ORDER BY ordinal_position"
        try:
            cursor.execute(sql_select_query)
            query_result = cursor.fetchall()
            for column_name_and_type in query_result:
                column_name = column_name_and_type[0]
                result.append(column_name)
        except mysql.connector.Error as err:
            self.logger.error(f"MySQL error message: {err.msg}")
        cursor.close()
        return result

    def getEncoders(self, database_name):
        cursor = self.connection.cursor()
        try:
            cursor.execute("USE " + database_name)
        except mysql.connector.Error as err:
            self.logger.error(f"MySQL error message: {err.msg}")
        sql_select_query = "SELECT encoder_name,encoder_object FROM encoder"
        try:
            cursor.execute(sql_select_query)
            result = cursor.fetchall()
        except mysql.connector.Error as err:
            self.logger.error(f"MySQL error message: {err.msg}")
        cursor.close()
        encoders = dict()
        for encoder_name, encoder_object in result:
            encoders[encoder_name] = pickle.loads(encoder_object)
        return encoders

    def get_records_with_limit_and_offset(self, database_name, table_name, limit, offset):
        result = dict()
        cursor = self.connection.cursor()
        sql_select_query = f"SELECT * FROM {database_name}.{table_name} limit {limit} offset {offset}"
        try:
            cursor.execute(sql_select_query)
            query_result = cursor.fetchall()
            for record in query_result:
                fields = list()
                length = len(record)
                id = 0
                for index in range(length):
                    if index == 0:
                        id = record[index]
                    else:
                        fields.append(record[index])
                result[id] = fields
        except mysql.connector.Error as err:
            self.logger.error(f"MySQL error message: {err.msg}")
        cursor.close()
        return result

    def get_schemas(self):
        cursor = self.connection.cursor()
        sql_select_query = "SHOW SCHEMAS"
        try:
            cursor.execute(sql_select_query)
            query_result = cursor.fetchall()
            logging.info(f"Schemas: {query_result}")
            cursor.close()
            return query_result
        except mysql.connector.Error as err:
            self.logger.error(f"MySQL error message: {err.msg}")

    def get_tables_of_given_database(self, database_name):
        cursor = self.connection.cursor()
        sql_select_query = f"SELECT distinct TABLE_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '{database_name}'"
        try:
            cursor.execute(sql_select_query)
            query_result = cursor.fetchall()
            logging.info(f"Tables of {database_name} schema: {query_result}")
            cursor.close()
            return query_result
        except mysql.connector.Error as err:
            self.logger.error(f"MySQL error message: {err.msg}")

    def get_detailed_information_about_table(self, schema_name, table_name):
        result = dict()
        cursor = self.connection.cursor()
        column_properties = list()
        original_data_types = list()
        column_name_and_type = dict()
        if table_name is None:
            self.logger.debug("No table name set, query the transaction table")
            table_name = "transaction"
        sql_select_query = f"SELECT COLUMN_NAME, DATA_TYPE  FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '{schema_name}' AND TABLE_NAME = '{table_name}' ORDER BY ordinal_position"
        try:
            cursor.execute(sql_select_query)
            query_result = cursor.fetchall()
            for column_name_and_type in query_result:
                column_property = dict()
                column_name = column_name_and_type[0]
                column_type_as_byte = column_name_and_type[1]
                column_type = column_type_as_byte.decode("utf-8")
                column_property["name"] = column_name
                column_property["type"] = column_type
                column_properties.append(column_property)
            result["fields"] = column_properties
        except mysql.connector.Error as err:
            self.logger.error(f"MySQL error message: {err.msg}")

        sql_select_query = f"SELECT COUNT(*) FROM {schema_name}.{table_name}"
        try:
            cursor.execute(sql_select_query)
            query_result = cursor.fetchone()
            result["record_number"] = query_result[0]
        except mysql.connector.Error as err:
            self.logger.error(f"MySQL error message: {err.msg}")
        sql_select_query = f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '{schema_name}' AND TABLE_NAME = '{table_name}' AND COLUMN_KEY='PRI'"
        try:
            cursor.execute(sql_select_query)
            query_result = cursor.fetchone()
            result["primary_key"] = query_result
        except mysql.connector.Error as err:
            self.logger.error(f"MySQL error message: {err.msg}")
        cursor.close()
        fraud_candidates = self.get_fraud_candidate_column_and_fraud_number(schema_name, table_name,
                                                                            result)
        result["fraud_candidates"] = fraud_candidates
        self.get_other_proper_column_data_type_to_encoding(schema_name, table_name, result)
        return result

    def get_fraud_candidate_column_and_fraud_number(self, schema_name, table_name, database_property_holder):
        candidate_fraud_columns = list()
        cursor = self.connection.cursor()
        column_names_and_types = database_property_holder.get("fields")
        for column_name_and_type in column_names_and_types:
            column_name = column_name_and_type.get("name")
            sql_select_query = f"SELECT {column_name} FROM {schema_name}.{table_name}"
            try:
                cursor.execute(sql_select_query)
                query_result = cursor.fetchall()
                candidate_fraud_column = dict()
                record_number = len(query_result)
                number_of_ones = 0
                number_of_nulls = 0
                for field in query_result:
                    if field[0] == 1 or field[0] == '1':
                        number_of_ones = number_of_ones + 1
                    elif field[0] == 0 or field[0] == '0':
                        number_of_nulls = number_of_nulls + 1
                if number_of_ones + number_of_nulls == record_number and number_of_ones != 0 and number_of_nulls != 0:
                    candidate_fraud_column["name"] = column_name
                    candidate_fraud_column["fraud_number"] = number_of_ones
                    candidate_fraud_column["no_fraud_number"] = number_of_nulls
                    candidate_fraud_columns.append(candidate_fraud_column)
            except mysql.connector.Error as err:
                self.logger.error(f"MySQL error message: {err.msg}")
        cursor.close()
        return candidate_fraud_columns

    def get_other_proper_column_data_type_to_encoding(self, schema_name, table_name, database_property_holder):
        cursor = self.connection.cursor()
        column_names_and_types = database_property_holder.get("fields")
        for column_name_and_type in column_names_and_types:
            column_name = column_name_and_type.get("name")
            column_type = column_name_and_type.get("type")
            other_eligible_type_for_encoding = list()
            if column_type.startswith("varchar"):
                other_eligible_type_for_encoding.clear()
                sql_select_query = f"SELECT {column_name} FROM {schema_name}.{table_name}"
                try:
                    cursor.execute(sql_select_query)
                    records = cursor.fetchall()
                    is_records_number_type = True
                    is_records_suited_int_type = True
                    for record_as_tuple in records:
                        record = record_as_tuple[0]
                        if not record.isnumeric():
                            is_records_number_type = False
                            is_records_suited_int_type = False
                            break
                        elif self.is_int(record):
                            value = int(record)
                            if value > 2147483647:
                                is_records_suited_int_type = False
                            else:
                                continue
                        elif self.is_float(record):
                            is_records_suited_int_type = False

                    if is_records_number_type:
                        other_eligible_type_for_encoding.append("float")
                    if is_records_suited_int_type:
                        other_eligible_type_for_encoding.append("int")
                except mysql.connector.Error as err:
                    self.logger.error(f"MySQL error message: {err.msg}")
            column_name_and_type["other_eligible_type_for_encoding"] = other_eligible_type_for_encoding
        cursor.close()

    def is_float(self, element):
        try:
            float(element)
            return True
        except ValueError:
            return False

    def is_int(self, element):
        try:
            int(element)
            return True
        except ValueError:
            return False

    def do_feature_engineering(self, schema_name, encoded_table_name, encoded_dataset, encoded_fields,
                               planned_feature_engineering_id, used_cpu_core):
        feature_engineering_parameters = self.get_feature_engineering_parameters_by_id(planned_feature_engineering_id)
        intervals = feature_engineering_parameters.get("intervals")
        feature_engineer = engineering_module.Engineer()
        feature_engineered_data_set, feature_engineering_parameters, feature_engineered_table_registry_id = \
            self.get_feature_engineered_data_set_based_on_schema_name_and_encoded_table_name_and_planned_fe_id(
                schema_name, encoded_table_name, planned_feature_engineering_id)
        if feature_engineered_data_set is None:
            feature_engineered_data_set = feature_engineer.create_new_features(
                encoded_dataset,
                encoded_fields,
                feature_engineering_parameters,
                used_cpu_core)
            feature_engineered_table_registry_id = self.create_table_and_persist_feature_engineered_dataset(schema_name,
                                                                                                            encoded_fields,
                                                                                                            encoded_table_name,
                                                                                                            planned_feature_engineering_id,
                                                                                                            intervals,
                                                                                                            feature_engineered_data_set)
        return feature_engineered_data_set, feature_engineering_parameters, feature_engineered_table_registry_id

    def get_feature_engineering_parameters_by_id(self, id):
        cursor = self.connection.cursor()
        sql_select_query = "SELECT feature_engineering_parameters FROM common_fraud.planned_feature_engineering WHERE id = %s"
        parameter = (id,)
        try:
            cursor.execute(sql_select_query, parameter)
            query_result = cursor.fetchone()
            cursor.close()
        except mysql.connector.Error as err:
            self.logger.error(f"MySQL error message: {err.msg}")
        result = json.loads(query_result[0])
        return result

    def get_feature_engineered_data_set_based_on_schema_name_and_encoded_table_name_and_planned_fe_id(self, schema_name,
                                                                                                      encoded_table_name,
                                                                                                      planned_feature_engineering_id):
        cursor = self.connection.cursor()
        sql_select_query = "SELECT feature_engineering_parameters FROM common_fraud.planned_feature_engineering WHERE id = %s"
        parameter = (planned_feature_engineering_id,)
        try:
            cursor.execute(sql_select_query, parameter)
            query_result = cursor.fetchone()
        except mysql.connector.Error as err:
            self.logger.error(f"MySQL error message: {err.msg}")
        feature_engineering_parameters = json.loads(query_result[0])
        sql_select_query = "SELECT feature_engineered_table_registry_id FROM common_fraud.planned_and_created_featured_engineering_table_version WHERE schema_name = %s and encoded_table_name = %s and planned_feature_engineering_id = %s"
        parameter = (schema_name, encoded_table_name, planned_feature_engineering_id)
        try:
            cursor.execute(sql_select_query, parameter)
            query_result = cursor.fetchone()
        except mysql.connector.Error as err:
            self.logger.error(f"MySQL error message: {err.msg}")
        feature_engineered_table_registry_id = query_result[0]
        if feature_engineered_table_registry_id is None:
            return None
        else:
            sql_select_query = "SELECT feature_engineered_table_name FROM common_fraud.feature_engineered_table_registry WHERE id = %s"
            parameter = (feature_engineered_table_registry_id,)
            try:
                cursor.execute(sql_select_query, parameter)
                query_result = cursor.fetchone()
            except mysql.connector.Error as err:
                self.logger.error(f"MySQL error message: {err.msg}")
            if query_result is None:
                return None
            else:
                feature_engineered_table_name = query_result[0]
                sql_select_query = "SELECT * FROM %s.%s"
                parameter = (schema_name, feature_engineered_table_name)
                try:
                    cursor.execute(sql_select_query, parameter)
                    query_result = cursor.fetchall()
                except mysql.connector.Error as err:
                    self.logger.error(f"MySQL error message: {err.msg}")
        result = np.array(query_result)
        cursor.close()
        return result, feature_engineering_parameters, feature_engineered_table_registry_id

    def create_table_and_persist_feature_engineered_dataset(self, schema_name, encoded_fields, encoded_table_name,
                                                            planned_feature_engineering_id, intervals,
                                                            feature_engineered_data_set):
        feature_engineered_table_name, feature_engineered_table_registry_id = self.create_feature_engineered_table_name_and_registry(
            schema_name, encoded_table_name, planned_feature_engineering_id)
        feature_engineered_fields = self.create_feature_engineered_table(schema_name, feature_engineered_table_name,
                                                                         encoded_fields, intervals)
        self.persist_feature_engineered_dataset(schema_name, feature_engineered_table_name, feature_engineered_fields,
                                                feature_engineered_data_set)
        return feature_engineered_table_registry_id

    def create_feature_engineered_table_name_and_registry(self, schema_name, encoded_table_name,
                                                          planned_feature_engineering_id):
        cursor = self.connection.cursor()
        sql_select_query = f"SELECT MAX(version) FROM common_fraud.feature_engineered_table_registry WHERE schema_name = {schema_name} and encoded_table_name = {encoded_table_name}"
        try:
            cursor.execute(sql_select_query)
            result = cursor.fetchone()
            cursor.close()
        except mysql.connector.Error as err:
            self.logger.error(f"MySQL error message: {err.msg}")
        if result is None:
            version = 1
        else:
            version = result + 1
        feature_engineered_table_name = "feature_engineered_v" + version + encoded_table_name
        feature_engineered_table_registry_id = self.insert_into_feature_engineered_table_registry(schema_name, version,
                                                                                                  encoded_table_name,
                                                                                                  feature_engineered_table_name)
        self.insert_into_planned_and_created_featured_engineering_table_version(schema_name, encoded_table_name,
                                                                                planned_feature_engineering_id,
                                                                                feature_engineered_table_registry_id)
        return feature_engineered_table_name, feature_engineered_table_registry_id

    def insert_into_feature_engineered_table_registry(self, schema_name, version, encoded_table_name,
                                                      feature_engineered_table_name):
        cursor = self.connection.cursor()
        sql_insert_query = "INSERT INTO common_fraud.feature_engineered_table_registry (schema_name, version, encoded_table_name, feature_engineered_table_name ) VALUES (%s,%s,%s,%s)"
        parameter = (schema_name, version, encoded_table_name, feature_engineered_table_name)
        try:
            cursor.execute(sql_insert_query, parameter)
            last_inserted_row_id = cursor.lastrowid
            self.connection.commit()
            cursor.close()
            self.logger.info(
                f"Feature engineered table name registered, schema name: {schema_name}, feature engineered table name: {feature_engineered_table_name}")
        except mysql.connector.Error as err:
            self.logger.error(f"MySQL error message: {err.msg}")
        return last_inserted_row_id

    def insert_into_planned_and_created_featured_engineering_table_version(self, schema_name, encoded_table_name,
                                                                           planned_feature_engineering_id,
                                                                           feature_engineered_table_registry_id):
        cursor = self.connection.cursor()
        sql_insert_query = "INSERT INTO common_fraud.planned_and_created_featured_engineering_table_version (schema_name, encoded_table_name, planned_feature_engineering_id, feature_engineered_table_registry_id) VALUES (%s,%s,%s,%s)"
        parameter = (
            schema_name, encoded_table_name, planned_feature_engineering_id, feature_engineered_table_registry_id)
        try:
            cursor.execute(sql_insert_query, parameter)
            last_inserted_row_id = cursor.lastrowid
            self.connection.commit()
            cursor.close()
            self.logger.info(
                f"Feature engineered table registry id inserted into planned_and_created_featured_engineering_table_version table, schema name: {schema_name}, encoded table name: {encoded_table_name}, planned_feature_engineering_id: {planned_feature_engineering_id}, feature_engineered_table_registry_id: {feature_engineered_table_registry_id}")
        except mysql.connector.Error as err:
            self.logger.error(f"MySQL error message: {err.msg}")
        cursor.close()

    def create_feature_engineered_table(self, schema_name, feature_engineered_table_name, encoded_fields, intervals):
        cursor = self.connection.cursor()
        generated_features = list()
        fields = list()
        fields.extend(encoded_fields)
        number_of_generated_features = len(intervals)
        for i in range(number_of_generated_features):
            feature_name = "feature_" + i
            generated_features.append(feature_name)
        fields.extend(generated_features)
        sql_create_script = f"CREATE TABLE IF NOT EXISTS {schema_name}.{feature_engineered_table_name} (id BIGINT NOT NULL AUTO_INCREMENT, "
        for field in encoded_fields:
            sql_create_script = sql_create_script + f"{field} DOUBLE, "
        number_of_fields = len(fields)
        for i in range(number_of_fields):
            sql_create_script = sql_create_script + fields[i] + "DOUBLE"
            if i != range - 1:
                sql_create_script = sql_create_script + ","
        sql_create_script = sql_create_script + " PRIMARY KEY (id)) engine = InnoDB"
        try:
            cursor.execute(sql_create_script)
            self.connection.commit()
            cursor.close()
        except mysql.connector.Error as err:
            self.logger.error(f"MySQL error message: {err.msg}")
        self.logger.info(
            f"Feature engineered table created, schema name: {schema_name}, feature_engineered_table_name: {feature_engineered_table_name}")
        return fields

    def persist_feature_engineered_dataset(self, schema_name, feature_engineered_table_name, feature_engineered_fields,
                                           extended_dataset):
        cursor = self.connection.cursor()
        fields_as_strings = "("
        sql_insert_script_parameters = "("
        number_of_fields = len(feature_engineered_fields)
        for i in range(number_of_fields):
            fields_as_strings = fields_as_strings + feature_engineered_fields[i]
            sql_insert_script_parameters = sql_insert_script_parameters + "%s"
            if i != number_of_fields - 1:
                fields_as_strings = fields_as_strings + ","
                sql_insert_script_parameters + ","
            else:
                fields_as_strings = fields_as_strings + ")"
                sql_insert_script_parameters = sql_insert_script_parameters + ")"

        sql_insert_script = f"INSERT INTO {schema_name}.{feature_engineered_table_name} " + fields_as_strings + " VALUES " + sql_insert_script_parameters

        values_array = np.array(extended_dataset)
        bound = 1000
        length = len(extended_dataset)
        if length > bound:
            numberOfPartArray = int(length / bound)
            numberOfRestDatas = length - numberOfPartArray * bound
            for i in range(0, numberOfPartArray, 1):
                tempArray = values_array[i * bound:(i + 1) * bound, :]
                valueList = list()
                for record in tempArray:
                    valueList.append(tuple(record))
                try:
                    cursor.executemany(sql_insert_script, valueList)
                    self.connection.commit()
                except mysql.connector.Error as err:
                    self.logger.error(f"MySQL error message: {err.msg}")
            tempArray = values_array[(numberOfPartArray) * bound:(numberOfPartArray) * bound + numberOfRestDatas, :]
            valueList = list()
            for record in tempArray:
                valueList.append(tuple(record))
            try:
                cursor.executemany(sql_insert_script, valueList)
                self.connection.commit()
            except mysql.connector.Error as err:
                self.logger.error(f"MySQL error message: {err.msg}")
        else:
            valueList = list()
            for record in values_array:
                valueList.append(tuple(record))
            try:
                cursor.executemany(sql_insert_script, valueList)
                self.connection.commit()
            except mysql.connector.Error as err:
                self.logger.error(f"MySQL error message: {err.msg}")
        cursor.close()
        self.logger.info(
            f"Feature engineered dataset persisted, schema name: {schema_name}, table name: {feature_engineered_table_name}")
