import logging
import math
import statistics
import multiprocessing as mp
from multiprocessing import Process, Pool
import numpy as np


class Engineer:
    def __init__(self):
        self.logger = logging.getLogger("train.server.database.engineer")

    def create_new_features(self, data_set, encoded_fields, feature_engineering_parameters, used_cpu_core):
        chosen_feature_field = feature_engineering_parameters.get("feature_name")
        feature_engineering_time_intervals = feature_engineering_parameters.get("intervals")
        self.logger.info(f"Used CPU logical core number: {used_cpu_core}")
        transaction_by_card_number = self.get_transaction_by_card_number(data_set, encoded_fields)
        input_collection = list()
        for key in transaction_by_card_number:
            input_dictionary = dict()
            input_dictionary["feature_field"] = chosen_feature_field
            input_dictionary["fields"] = encoded_fields
            input_dictionary["data"] = transaction_by_card_number.get(key)
            input_dictionary["intervals"] = feature_engineering_time_intervals
            input_collection.append(input_dictionary)
        result = list()
        with Pool(processes=used_cpu_core) as pool:
            result_from_threads = pool.map(self.process_dataset_of_single_cardnumber, input_collection)
            self.logger.info(f"Number of sublists from threads: {len(result_from_threads)}")
            for sub_list in result_from_threads:
                result.extend(sub_list)
        return result

    def get_transaction_by_card_number(self, data_set, encoded_fields):
        result = dict()
        for record in data_set:
            index = encoded_fields.index(card_number)
            card_number = record[index]
            if result.get(card_number) is None:
                transactions = list()
                transactions.append(record.tolist())
                result[card_number] = transactions
            else:
                transactions = result.get(card_number)
                transactions.append(record.tolist())
        return result

    def process_dataset_of_single_cardnumber(self, input_data_of_given_card_number):
        chosen_feature_field = input_data_of_given_card_number.get("feature_field")
        encoded_fields = input_data_of_given_card_number.get("fields")
        raw_data_set = input_data_of_given_card_number.get("data")
        feature_engineering_time_intervals = input_data_of_given_card_number.get("intervals")
        number_of_new_feature = len(feature_engineering_time_intervals) * 14
        extended_dataset = list()
        data_set = np.array(raw_data_set)
        transaction_features = data_set[:, 1:-1]
        transaction_labels = data_set[:, -1:]
        length = len(transaction_features)
        for i in range(length):
            transaction_feature = transaction_features[i]
            index_of_card_number = encoded_fields.index(chosen_feature_field)
            current_card_number = math.floor(transaction_feature[index_of_card_number])
            index_of_timestamp = encoded_fields.index(chosen_feature_field)
            current_time_stamp = transaction_feature[index_of_timestamp]
            index_of_feature_to_engineer = encoded_fields.index(chosen_feature_field)
            feature_value = transaction_feature[index_of_feature_to_engineer]
            current_amount = transaction_feature[3]
            transaction_feature_list = list(transaction_feature)

            if i == length - 1:
                for j in range(number_of_new_feature):
                    transaction_feature_list.append(0)
                extended_dataset.append(transaction_feature_list)
            else:
                feature_to_engineer_and_timestamp_collection = self.get_all_feature_and_timestamp_before_given_timestamp(
                    transaction_features, current_time_stamp, chosen_feature_field, encoded_fields)
                # amount_and_time_stamp_collection = self.get_amount_and_time_stamp_collection_before_given_time_stamp(
                #     transaction_features, current_time_stamp)

                generated_features = list()

                transaction_feature_properties = self.get_transaction_feature_properties(current_time_stamp,
                                                                                         feature_value,
                                                                                         feature_to_engineer_and_timestamp_collection,
                                                                                         chosen_feature_field,
                                                                                         encoded_fields,
                                                                                         feature_engineering_time_intervals)

                transaction_number_properties = self.get_transaction_number_properties(current_time_stamp,
                                                                                       feature_to_engineer_and_timestamp_collection,
                                                                                       feature_engineering_time_intervals)
                transaction_interval_properties = self.get_transaction_interval_properties(current_time_stamp,
                                                                                           feature_to_engineer_and_timestamp_collection,
                                                                                           feature_engineering_time_intervals)
                generated_features.extend(transaction_feature_properties)
                generated_features.extend(transaction_number_properties)
                generated_features.extend(transaction_interval_properties)
                number_of_generated_features = len(generated_features)
                for interval in feature_engineering_time_intervals:
                    for k in range(number_of_generated_features):
                        transaction_feature_list.append(generated_features[k].get(interval))

                label = transaction_labels[i][0]
                transaction_feature_list.append(label)
                transaction_feature_tuple = tuple(transaction_feature_list)
                extended_dataset.append(transaction_feature_tuple)
        return extended_dataset


    def get_all_feature_and_timestamp_before_given_timestamp(self, data_set, given_time_stamp, chosen_feature_field,
                                                             encoded_fields):
        result = list()
        for record in data_set:
            index_of_timestamp = encoded_fields.index("timestamp")
            current_time_stamp = record[index_of_timestamp]
            index_of_current_feature = encoded_fields.index(chosen_feature_field)
            current_feature = record[index_of_current_feature]
            if current_time_stamp < given_time_stamp:
                result.append((current_feature, current_time_stamp))
        return result

    def get_transaction_feature_properties(self, current_timestamp, feature_value,
                                           feature_to_engineer_and_timestamp_collection, chosen_feature_field,
                                           encoded_fields, feature_engineering_time_intervals):
        # timestamp, amount, amount_and_timestamp_collection
        amount_collection_by_date_dictionary = dict()
        feature_collection_by_date_dictionary = dict()

        for current_feature_and_timestamp in feature_to_engineer_and_timestamp_collection:
            current_feature = current_feature_and_timestamp[0]
            timestamp = current_feature_and_timestamp[1]
            current_date = int(timestamp)
            if feature_collection_by_date_dictionary.get(current_date) is None:
                feature_collection = list()
                feature_collection.append(current_feature)
                feature_collection_by_date_dictionary[current_date] = feature_collection
        else:
            feature_collection = feature_collection_by_date_dictionary.get(current_date)
            feature_collection.append(current_feature)

        feature_items_by_retrospective_time = dict()
        for interval in feature_engineering_time_intervals:
            feature_items_by_retrospective_time[interval] = list()

        #
        #
        # transactionDailyAmountCollectionSince3Days = list()
        # transactionDailyAmountCollectionSince7Days = list()
        # transactionDailyAmountCollectionSince15Days = list()
        # transactionDailyAmountCollectionSince30Days = list()
        # transactionDailyAmountCollectionSinceFirstDate = list()

        for date in feature_collection_by_date_dictionary.keys():
            for interval in feature_engineering_time_intervals:
                if date > current_timestamp - interval:
                    feature_items_by_retrospective_time.get(interval).append(
                        feature_collection_by_date_dictionary.get(date))

        # for date in amount_collection_by_date_dictionary.keys():
        #     if date > timestamp - 3:
        #         transactionDailyAmountCollectionSince3Days.extend(amount_collection_by_date_dictionary.get(date))
        #     if date > timestamp - 7:
        #         transactionDailyAmountCollectionSince7Days.extend(amount_collection_by_date_dictionary.get(date))
        #     if date > timestamp - 15:
        #         transactionDailyAmountCollectionSince15Days.extend(amount_collection_by_date_dictionary.get(date))
        #     if date > timestamp - 30:
        #         transactionDailyAmountCollectionSince30Days.extend(amount_collection_by_date_dictionary.get(date))
        #     transactionDailyAmountCollectionSinceFirstDate.extend(amount_collection_by_date_dictionary.get(date))

        average_feature_value_by_retrospective_time = dict()
        for interval in feature_engineering_time_intervals:
            average_feature_value_by_retrospective_time[interval] = 0

        deviation_feature_value_by_retrospective_time = dict()
        for interval in feature_engineering_time_intervals:
            deviation_feature_value_by_retrospective_time[interval] = 0

        median_feature_value_by_retrospective_time = dict()
        for interval in feature_engineering_time_intervals:
            median_feature_value_by_retrospective_time[interval] = 0

        for interval in average_feature_value_by_retrospective_time.keys():
            average_feature_value_by_retrospective_time[interval] = statistics.mean(
                feature_items_by_retrospective_time.get(interval)) if len(
                feature_items_by_retrospective_time.get(interval)) > 0 else 0
            deviation_feature_value_by_retrospective_time[interval] = statistics.stdev(
                feature_items_by_retrospective_time.get(interval)) if len(
                feature_items_by_retrospective_time.get(interval)) > 0 else 0
            median_feature_value_by_retrospective_time[interval] = statistics.median(
                feature_items_by_retrospective_time.get(interval)) if len(
                feature_items_by_retrospective_time.get(interval)) > 0 else 0

        feature_per_average_feature_value_by_retrospective_time = dict()
        feature_minus_average_feature_value_by_retrospective_time = dict()
        feature_per_median_feature_value_by_retrospective_time = dict()
        feature_minus_median_feature_value_by_retrospective_time = dict()
        feature_minus_average_feature_per_median_feature_value_by_retrospective_time = dict()
        feature_minus_average_feature_minus_median_feature_value_by_retrospective_time = dict()

        for interval in feature_engineering_time_intervals:
            feature_per_average_feature_value_by_retrospective_time[
                interval] = feature_value / average_feature_value_by_retrospective_time.get(
                interval) if average_feature_value_by_retrospective_time.get(interval) != 0 else 0
            feature_minus_average_feature_value_by_retrospective_time[
                interval] = feature_value - average_feature_value_by_retrospective_time.get(interval)
            feature_per_median_feature_value_by_retrospective_time[
                interval] = feature_value / median_feature_value_by_retrospective_time.get(
                interval) if median_feature_value_by_retrospective_time.get(interval) != 0 else 0
            feature_minus_median_feature_value_by_retrospective_time[
                interval] = feature_value - median_feature_value_by_retrospective_time.get(interval)
            feature_minus_average_feature_per_median_feature_value_by_retrospective_time[
                interval] = feature_value / average_feature_value_by_retrospective_time.get(
                interval) if average_feature_value_by_retrospective_time.get(interval) != 0 else 0
            feature_minus_average_feature_minus_median_feature_value_by_retrospective_time[
                interval] = feature_value - average_feature_value_by_retrospective_time.get(interval)
        return [feature_per_average_feature_value_by_retrospective_time,
                feature_minus_average_feature_value_by_retrospective_time,
                feature_per_median_feature_value_by_retrospective_time,
                feature_minus_median_feature_value_by_retrospective_time,
                feature_minus_average_feature_per_median_feature_value_by_retrospective_time,
                feature_minus_average_feature_minus_median_feature_value_by_retrospective_time]

    def get_transaction_number_properties(self, timestamp, feature_to_engineer_and_timestamp_collection,
                                          feature_engineering_time_intervals):
        actual_date = int(timestamp)
        timestamp_collection = list()
        for item in feature_to_engineer_and_timestamp_collection:
            timestamp_collection.append(item[1])

        daily_transaction_numbers_by_retrospective_time = dict()
        for interval in feature_engineering_time_intervals:
            daily_transaction_numbers_by_retrospective_time[interval] = list()

        transaction_number_by_date = dict()
        transaction_number_by_date[actual_date] = 1
        for current_timestamp in timestamp_collection:
            current_date = int(current_timestamp)
            if transaction_number_by_date.get(current_date) is None:
                transaction_number_by_date[current_date] = 1
            else:
                number = transaction_number_by_date.get(current_date)
                number = number + 1
                transaction_number_by_date[current_date] = number

        transaction_number_on_current_day = transaction_number_by_date.get(actual_date)

        if transaction_number_on_current_day is None:
            print("transactionNumberOnCurrentDay NONE")
            transaction_number_on_current_day = 1

        for date in transaction_number_by_date.keys():
            for interval in feature_engineering_time_intervals:
                if date > timestamp - interval:
                    daily_transaction_numbers_by_retrospective_time.get(interval).append(
                        transaction_number_by_date.get(date))

        average_transaction_number_by_retrospective_time = dict()
        median_transaction_number_by_retrospective_time = dict()

        for interval in feature_engineering_time_intervals:
            average_transaction_number_by_retrospective_time[interval] = statistics.mean(
                daily_transaction_numbers_by_retrospective_time.get(
                    interval)) if daily_transaction_numbers_by_retrospective_time.get(interval) > 0 else 0
            median_transaction_number_by_retrospective_time[interval] = statistics.median(
                daily_transaction_numbers_by_retrospective_time.get(
                    interval)) if daily_transaction_numbers_by_retrospective_time.get(interval) > 0 else 0

        transaction_number_on_current_day_per_daily_average_transaction_number_by_retrospective_time = dict()
        transaction_number_on_current_day_minus_daily_average_transaction_number_by_retrospective_time = dict()
        transaction_number_on_current_day_per_daily_median_transaction_number_by_retrospective_time = dict()
        transaction_number_on_current_day_minus_daily_median_transaction_number_by_retrospective_time = dict()
        for interval in feature_engineering_time_intervals:
            transaction_number_on_current_day_per_daily_average_transaction_number_by_retrospective_time[
                interval] = transaction_number_on_current_day / average_transaction_number_by_retrospective_time.get(
                interval) if average_transaction_number_by_retrospective_time.get(interval) != 0 else 0
            transaction_number_on_current_day_minus_daily_average_transaction_number_by_retrospective_time[
                interval] = transaction_number_on_current_day - average_transaction_number_by_retrospective_time.get(
                interval)
            transaction_number_on_current_day_per_daily_median_transaction_number_by_retrospective_time[
                interval] = transaction_number_on_current_day / median_transaction_number_by_retrospective_time.get(
                interval) if median_transaction_number_by_retrospective_time.get(interval) != 0 else 0
            transaction_number_on_current_day_minus_daily_median_transaction_number_by_retrospective_time[
                interval] = transaction_number_on_current_day - median_transaction_number_by_retrospective_time.get(
                interval)
        return [transaction_number_on_current_day_per_daily_average_transaction_number_by_retrospective_time,
                transaction_number_on_current_day_minus_daily_average_transaction_number_by_retrospective_time,
                transaction_number_on_current_day_per_daily_median_transaction_number_by_retrospective_time,
                transaction_number_on_current_day_minus_daily_median_transaction_number_by_retrospective_time]

    def get_transaction_interval_properties(self, timestamp, feature_to_engineer_and_timestamp_collection,
                                            feature_engineering_time_intervals):
        timestamp_collection = list()
        for item in feature_to_engineer_and_timestamp_collection:
            timestamp_collection.append(item[1])
        length_of_timestamp_collection = len(timestamp_collection)
        current_distance_between_transactions = timestamp - timestamp_collection[
            0] if length_of_timestamp_collection >= 1 else 0
        distance_between_transactions_by_retrospective_time = dict()
        for interval in feature_engineering_time_intervals:
            distance_between_transactions_by_retrospective_time[interval] = list()
        for i in range(0, length_of_timestamp_collection - 1, 1):
            next_time_stamp = timestamp_collection[i + 1]
            current_time_stamp = timestamp_collection[i]
            distance_between_transactions = current_time_stamp - next_time_stamp
            for interval in feature_engineering_time_intervals:
                if current_time_stamp > timestamp - interval:
                    distance_between_transactions_by_retrospective_time.get(interval).append(
                        distance_between_transactions)

        average_distance_between_transactions_by_retrospective_time = dict()
        median_distance_between_transactions_by_retrospective_time = dict()
        for interval in feature_engineering_time_intervals:
            average_distance_between_transactions_by_retrospective_time[interval] = statistics.mean(
                distance_between_transactions_by_retrospective_time.get(interval)) if len(
                distance_between_transactions_by_retrospective_time.get(interval)) > 0 else 0
            median_distance_between_transactions_by_retrospective_time[interval] = statistics.median(
                distance_between_transactions_by_retrospective_time.get(interval)) if len(
                distance_between_transactions_by_retrospective_time.get(interval)) > 0 else 0

        average_time_distance_upon_retrospective_time_per_current_distance_between_transaction_by_retrospective_time = dict()
        average_time_distance_upon_retrospective_time_minus_current_distance_between_transaction_by_retrospective_time = dict()
        median_time_distance_upon_retrospective_time_per_current_distance_between_transaction_by_retrospective_time = dict()
        median_time_distance_upon_retrospective_time_minus_current_distance_between_transaction_by_retrospective_time = dict()

        for interval in feature_engineering_time_intervals:
            average_time_distance_upon_retrospective_time_per_current_distance_between_transaction_by_retrospective_time[
                interval] = average_distance_between_transactions_by_retrospective_time.get(
                interval) / current_distance_between_transactions if current_distance_between_transactions != 0 else 0
            average_time_distance_upon_retrospective_time_minus_current_distance_between_transaction_by_retrospective_time[
                interval] = average_distance_between_transactions_by_retrospective_time.get(
                interval) - current_distance_between_transactions
            median_time_distance_upon_retrospective_time_per_current_distance_between_transaction_by_retrospective_time[
                interval] = median_distance_between_transactions_by_retrospective_time.get(
                interval) / current_distance_between_transactions if current_distance_between_transactions != 0 else 0
            median_time_distance_upon_retrospective_time_minus_current_distance_between_transaction_by_retrospective_time[
                interval] = median_distance_between_transactions_by_retrospective_time.get(
                interval) - current_distance_between_transactions

        return [
            average_time_distance_upon_retrospective_time_per_current_distance_between_transaction_by_retrospective_time,
            average_time_distance_upon_retrospective_time_minus_current_distance_between_transaction_by_retrospective_time,
            median_time_distance_upon_retrospective_time_per_current_distance_between_transaction_by_retrospective_time,
            median_time_distance_upon_retrospective_time_minus_current_distance_between_transaction_by_retrospective_time]
