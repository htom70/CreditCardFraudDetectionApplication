import logging
import mysql.connector
import numpy as np

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
        self.logger = logging.getLogger("estimator.server.database")
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
        except mysql.connector.Error as err:
            self.logger.error("MySQL connection error: " + err.msg)
        connection.set_converter_class(NumpyMySQLConverter)
        self.logger.info("Database connection created")
        return connection

    def load_estimator_container(self, estimator_id):
        cursor = self.connection.cursor()
        try:
            cursor.execute("USE train")
        except mysql.connector.Error as err:
            self.logger.error("MySQL use error: ", err)
        sql_select_query = f"SELECT estimator_object FROM estimator WHERE id = %s"
        parameter = (estimator_id,)
        try:
            cursor.execute(sql_select_query, parameter)
        except mysql.connector.Error as err:
            self.logger.error("MySQL use error: ", err)
        result = cursor.fetchone()
        cursor.close()
        if result is not None:
            pickled_estimator_container = result[0]
        else:
            pickled_estimator_container = None
        return pickled_estimator_container

    def get_earlier_amount_and_time_stamp_collection(self, database_name, given_card_number,
                                                     given_timestamp):
        cursor = self.connection.cursor()
        try:
            cursor.execute(f"USE {database_name}")
        except mysql.connector.Error as err:
            self.logger.error("MySQL use error: ", err)
        sql_select_query = "SELECT amount, timestamp FROM encoded_transaction WHERE card_number = %s and timestamp < %s"
        parameter = (given_card_number, given_timestamp)
        try:
            cursor.execute(sql_select_query, parameter)
        except mysql.connector.Error as err:
            self.logger.error("MySQL use error: ", err)
        records = cursor.fetchall()
        cursor.close()
        return np.array(records)

    def is_database_exist(self, database_name):
        cursor = self.connection.cursor()
        try:
            cursor.execute(f"USE {database_name}")
        except mysql.connector.Error as err:
            self.logger.error("MySQL use error: ", err)
        sql_select_query = "SHOW SCHEMAS LIKE %s"
        parameter = (database_name,)
        try:
            cursor.execute(sql_select_query, parameter)
        except mysql.connector.Error as err:
            self.logger.error("MySQL use error: ", err)
        record = cursor.fetchone()
        cursor.close()
        if record is None:
            result = False
        elif len(record) == 1:
            result = True
        else:
            result = False
        return result

    def is_feature_engineered_table_exist(self, database_name):
        cursor = self.connection.cursor()
        try:
            cursor.execute(f"USE {database_name}")
        except mysql.connector.Error as err:
            self.logger.error("MySQL use error: ", err)
        sql_select_query = "SHOW TABLES LIKE %s"
        parameter = ("feature_engineered_transaction",)
        try:
            cursor.execute(sql_select_query, parameter)
        except mysql.connector.Error as err:
            self.logger.error("MySQL use error: ", err)
        record = cursor.fetchone()
        cursor.close()
        if record is None:
            result = False
        elif len(record) == 1:
            result = True
        else:
            result = False
        return result

    def save_feature_engineered_dataset(self, database_name, extended_dataset):
        cursor = self.connection.cursor()
        try:
            cursor.execute(f"USE {database_name}")
        except mysql.connector.Error as err:
            self.logger.error("MySQL use error: ", err)
        insert_script_file = open("SQL INSERT feature_engineered_transaction.txt", "r")
        sql_insert_script = insert_script_file.read()
        try:
            cursor.execute(sql_insert_script, extended_dataset)
        except mysql.connector.Error as err:
            self.logger.error("MySQL use error: ", err)
        self.connection.commit()
        cursor.close()
        self.logger.info(f"The new record inserted in the database: {database_name}")
