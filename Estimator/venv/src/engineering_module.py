import logging
import math
import statistics
import multiprocessing as mp
from multiprocessing import Process, Pool
import numpy as np


class Engineer:
    def __init__(self):
        self.logger = logging.getLogger("estimator.server.engineer")


    def create_new_features(self, data_set, used_cpu_core):
        self.logger.info(f"Used CPU logical core number: {used_cpu_core}")
        transaction_by_card_number = self.get_transaction_by_card_number(data_set)
        # result = list()
        # number_of_cards= len(transaction_by_card_number.keys())
        # group_number_of_card=int(number_of_cards/used_cpu_core)
        # number_of_rest_card=number_of_cards-group_number_of_card*used_cpu_core
        # sub_dataset_holder=list()
        # for i in range(group_number_of_card):
        #     sub_dataset=list()
        #

        # process_dictionary = dict()
        # result_from_processes_dictionary = dict()
        # dataset_from_processes = list()
        # i = 0
        # with mp.Manager() as manager:
        #     for key in transaction_by_card_number.keys():
        #         result_from_processes_dictionary[i] = manager.list()
        #         result_list = result_from_processes_dictionary.get(i)
        #         dataset_of_single_card = transaction_by_card_number.get(key)
        #         process_dictionary[i] = Process(target=self.process_dataset_of_single_cardnumber,
        #                                         args=(dataset_of_single_card, result_list))
        #         i = i + 1
        #     for j in range(i):
        #         process_dictionary.get(j).start()
        #     # process_dictionary.get(0).start()
        #     for j in range(i):
        #         process_dictionary.get(j).join()
        #     # process_dictionary.get(0).join()
        #     for j in range(i):
        #         dataset_from_processes.extend(result_from_processes_dictionary.get(j))
        #     # dataset_from_processes.extend(result_from_processes_dictionary.get(0))
        #     print(len(dataset_from_processes))
        # return dataset_from_processes

        # with mp.Manager() as manager:
        #     for i in range(used_cpu_core):
        #         result_from_processes_dictionary[i] = manager.list()
        #         result_list = result_from_processes_dictionary.get(i)

        # transactions=transaction_by_card_number.values()
        # for key in transaction_by_card_number.keys():
        #     transactions.extend(transaction_by_card_number.get(key))
        transactions=list()
        transactions.append(transaction_by_card_number.get(6876024694415205))
        # transactions.append(transaction_by_card_number.get(9833232336877046))
        result=list()
        with Pool(processes=used_cpu_core) as pool:
            result_from_threads = pool.map(self.process_dataset_of_single_cardnumber, transaction_by_card_number.values())
            print(len(result))
            print("End")
            self.logger.info(f"Number of sublists from threads: {len(result_from_threads)}")
            for sub_list in result_from_threads:
                result.extend(sub_list)
            for item in result:
                print(len(item))
        print(len(result))
        return result

    def get_transaction_by_card_number(self, data_set):
        result = dict()
        for record in data_set:
            card_number = record[1]
            if result.get(card_number) is None:
                transactions = list()
                transactions.append(record.tolist())
                result[card_number] = transactions
            else:
                transactions = result.get(card_number)
                transactions.append(record.tolist())
        return result

    def get_engineered_transaction(self, encoded_data_set, earlier_amount_and_timestamps):

        current_time_stamp = encoded_data_set[2]
        current_amount = encoded_data_set[3]
        engineered_transaction_feature_list = list(encoded_data_set)

        transaction_amount_properties = self.get_transaction_amount_properties(current_time_stamp,
                                                                                       current_amount,
                                                                                       earlier_amount_and_timestamps)
        transaction_number_properties = self.get_transaction_number_properties(current_time_stamp,
                                                                                       earlier_amount_and_timestamps)
        transaction_interval_properties = self.get_transaction_interval_properties(current_time_stamp,
                                                                                           earlier_amount_and_timestamps)
        keyAppends = ["3Days", "7Days", "15Days", "30Days", "FirstDay"]

        for keyAppend in keyAppends:
            engineered_transaction_feature_list.append(transaction_amount_properties.get(f"amountToAverageAmountRatioSince{keyAppend}"))
            engineered_transaction_feature_list.append(transaction_amount_properties.get(f"amountToAverageAmountDiffSince{keyAppend}"))
            engineered_transaction_feature_list.append(transaction_amount_properties.get(f"amountToMedianAmountRatioSince{keyAppend}"))
            engineered_transaction_feature_list.append(transaction_amount_properties.get(f"amountToMedianAmountDiffSince{keyAppend}"))
            engineered_transaction_feature_list.append(transaction_amount_properties.get(f"amountMinusAverageAmountToDeviationAmountRatioSince{keyAppend}"))
            engineered_transaction_feature_list.append(transaction_amount_properties.get(f"amountMinusAverageAmountToDeviationAmountDiffSince{keyAppend}"))

            engineered_transaction_feature_list.append(transaction_number_properties.get(f"transactionNumberToAverageDailyTransactionNumberRatioSince{keyAppend}"))
            engineered_transaction_feature_list.append(transaction_number_properties.get(f"transactionNumberToAverageDailyTransactionNumberDiffSince{keyAppend}"))
            engineered_transaction_feature_list.append(transaction_number_properties.get(f"transactionNumberToMedianDailyTransactionNumberRatioSince{keyAppend}"))
            engineered_transaction_feature_list.append(transaction_number_properties.get(f"transactionNumberToAverageDailyTransactionNumberDiffSince{keyAppend}"))

            engineered_transaction_feature_list.append(transaction_interval_properties.get(f"intervalToAverageIntervalRatioSince{keyAppend}"))
            engineered_transaction_feature_list.append(transaction_interval_properties.get(f"intervalToAverageIntervalDiffSince{keyAppend}"))
            engineered_transaction_feature_list.append(transaction_interval_properties.get(f"intervalToMedianIntervalRatioSince{keyAppend}"))
            engineered_transaction_feature_list.append(transaction_interval_properties.get(f"intervalToMedianIntervalDiffSince{keyAppend}"))

        engineered_transaction_feature_list.append(transaction_interval_properties.get("time_stamp_difference"))

        return engineered_transaction_feature_list

    # def get_amount_and_time_stamp_collection_before_given_time_stamp(self, data_set, given_time_stamp):
    #     result = list()
    #     for record in data_set:
    #         current_time_stamp = record[2]
    #         current_amount = record[3]
    #         if current_time_stamp < given_time_stamp:
    #             result.append((current_amount, current_time_stamp))
    #     return result

    def get_transaction_amount_properties(self, timestamp, amount, amount_and_timestamp_collection):
        amount_collection_by_date_dictionary = dict()

        for current_amount_and_timestamp in amount_and_timestamp_collection:
            current_amount = current_amount_and_timestamp[0]
            current_timestamp = current_amount_and_timestamp[1]
            current_date = int(current_timestamp)
            if amount_collection_by_date_dictionary.get(current_date) is None:
                amount_collection = list()
                amount_collection.append(current_amount)
                amount_collection_by_date_dictionary[current_date] = amount_collection
            else:
                amount_collection = amount_collection_by_date_dictionary.get(current_date)
                amount_collection.append(current_amount)

        transactionDailyAmountCollectionSince3Days = list()
        transactionDailyAmountCollectionSince7Days = list()
        transactionDailyAmountCollectionSince15Days = list()
        transactionDailyAmountCollectionSince30Days = list()
        transactionDailyAmountCollectionSinceFirstDate = list()

        for date in amount_collection_by_date_dictionary.keys():
            if date > timestamp - 3:
                transactionDailyAmountCollectionSince3Days.extend(amount_collection_by_date_dictionary.get(date))
            if date > timestamp - 7:
                transactionDailyAmountCollectionSince7Days.extend(amount_collection_by_date_dictionary.get(date))
            if date > timestamp - 15:
                transactionDailyAmountCollectionSince15Days.extend(amount_collection_by_date_dictionary.get(date))
            if date > timestamp - 30:
                transactionDailyAmountCollectionSince30Days.extend(amount_collection_by_date_dictionary.get(date))
            transactionDailyAmountCollectionSinceFirstDate.extend(amount_collection_by_date_dictionary.get(date))

        averageAmountSince3Days = statistics.mean(transactionDailyAmountCollectionSince3Days) if len(
            transactionDailyAmountCollectionSince3Days) > 0 else 0
        deviationAmountSince3Days = statistics.stdev(transactionDailyAmountCollectionSince3Days) if len(
            transactionDailyAmountCollectionSince3Days) > 1 else 0
        medianAmountSince3Days = statistics.median(transactionDailyAmountCollectionSince3Days) if len(
            transactionDailyAmountCollectionSince3Days) > 0 else 0
        amountToAverageAmountRatioSince3Days = amount / averageAmountSince3Days if averageAmountSince3Days != 0 else 0
        amountToAverageAmountDiffSince3Days = amount - averageAmountSince3Days
        amountToMedianAmountRatioSince3Days = amount / medianAmountSince3Days if medianAmountSince3Days != 0 else 0
        amountToMedianAmountDiffSince3Days = amount - medianAmountSince3Days
        amountMinusAverageAmountToDeviationAmountRatioSince3Days = (
                                                                           amount - averageAmountSince3Days) / deviationAmountSince3Days if deviationAmountSince3Days != 0 else 0
        amountMinusAverageAmountToDeviationAmountDiffSince3Days = amount - averageAmountSince3Days - deviationAmountSince3Days

        averageAmountSince7Days = statistics.mean(transactionDailyAmountCollectionSince7Days) if len(
            transactionDailyAmountCollectionSince7Days) > 0 else 0
        deviationAmountSince7Days = statistics.stdev(transactionDailyAmountCollectionSince7Days) if len(
            transactionDailyAmountCollectionSince7Days) > 1 else 0
        medianAmountSince7Days = statistics.median(transactionDailyAmountCollectionSince7Days) if len(
            transactionDailyAmountCollectionSince7Days) > 0 else 0
        amountToAverageAmountRatioSince7Days = amount / averageAmountSince7Days if averageAmountSince7Days != 0 else 0
        amountToAverageAmountDiffSince7Days = amount - averageAmountSince7Days
        amountToMedianAmountRatioSince7Days = amount / medianAmountSince7Days if medianAmountSince7Days != 0 else 0
        amountToMedianAmountDiffSince7Days = amount - medianAmountSince7Days
        amountMinusAverageAmountToDeviationAmountRatioSince7Days = (
                                                                           amount - averageAmountSince7Days) / deviationAmountSince7Days if deviationAmountSince7Days != 0 else 0
        amountMinusAverageAmountToDeviationAmountDiffSince7Days = amount - averageAmountSince7Days - deviationAmountSince7Days

        averageAmountSince15Days = statistics.mean(transactionDailyAmountCollectionSince15Days) if len(
            transactionDailyAmountCollectionSince15Days) > 0 else 0
        deviationAmountSince15Days = statistics.stdev(transactionDailyAmountCollectionSince15Days) if len(
            transactionDailyAmountCollectionSince15Days) > 1 else 0
        medianAmountSince15Days = statistics.median(transactionDailyAmountCollectionSince15Days) if len(
            transactionDailyAmountCollectionSince15Days) > 0 else 0
        amountToAverageAmountRatioSince15Days = amount / averageAmountSince15Days if averageAmountSince15Days != 0 else 0
        amountToAverageAmountDiffSince15Days = amount - averageAmountSince15Days
        amountToMedianAmountRatioSince15Days = amount / medianAmountSince15Days if medianAmountSince15Days != 0 else 0
        amountToMedianAmountDiffSince15Days = amount - medianAmountSince15Days
        amountMinusAverageAmountToDeviationAmountRatioSince15Days = (
                                                                            amount - averageAmountSince15Days) / deviationAmountSince15Days if deviationAmountSince15Days != 0 else 0
        amountMinusAverageAmountToDeviationAmountDiffSince15Days = amount - averageAmountSince15Days - deviationAmountSince15Days

        averageAmountSince30Days = statistics.mean(transactionDailyAmountCollectionSince30Days) if len(
            transactionDailyAmountCollectionSince30Days) > 0 else 0
        deviationAmountSince30Days = statistics.stdev(transactionDailyAmountCollectionSince30Days) if len(
            transactionDailyAmountCollectionSince30Days) > 1 else 0
        medianAmountSince30Days = statistics.median(transactionDailyAmountCollectionSince30Days) if len(
            transactionDailyAmountCollectionSince30Days) > 0 else 0
        amountToAverageAmountRatioSince30Days = amount / averageAmountSince30Days if averageAmountSince30Days != 0 else 0
        amountToAverageAmountDiffSince30Days = amount - averageAmountSince30Days
        amountToMedianAmountRatioSince30Days = amount / medianAmountSince30Days if medianAmountSince30Days != 0 else 0
        amountToMedianAmountDiffSince30Days = amount - medianAmountSince30Days
        amountMinusAverageAmountToDeviationAmountRatioSince30Days = (
                                                                            amount - averageAmountSince30Days) / deviationAmountSince30Days if deviationAmountSince30Days != 0 else 0
        amountMinusAverageAmountToDeviationAmountDiffSince30Days = amount - averageAmountSince30Days - deviationAmountSince30Days

        averageAmountSinceFirstDay = statistics.mean(transactionDailyAmountCollectionSinceFirstDate) if len(
            transactionDailyAmountCollectionSinceFirstDate) > 0 else 0
        deviationAmountSinceFirstDay = statistics.stdev(transactionDailyAmountCollectionSinceFirstDate) if len(
            transactionDailyAmountCollectionSinceFirstDate) > 1 else 0
        medianAmountSinceFirstDay = statistics.median(transactionDailyAmountCollectionSinceFirstDate) if len(
            transactionDailyAmountCollectionSinceFirstDate) > 0 else 0
        amountToAverageAmountRatioSinceFirstDay = amount / averageAmountSinceFirstDay if averageAmountSinceFirstDay != 0 else 0
        amountToAverageAmountDiffSinceFirstDay = amount - averageAmountSinceFirstDay
        amountToMedianAmountRatioSinceFirstDay = amount / medianAmountSinceFirstDay if medianAmountSinceFirstDay != 0 else 0
        amountToMedianAmountDiffSinceFirstDay = amount - medianAmountSinceFirstDay
        amountMinusAverageAmountToDeviationAmountRatioSinceFirstDay = (
                                                                              amount - averageAmountSinceFirstDay) / deviationAmountSinceFirstDay if deviationAmountSinceFirstDay != 0 else 0
        amountMinusAverageAmountToDeviationAmountDiffSinceFirstDay = amount - averageAmountSinceFirstDay - deviationAmountSinceFirstDay

        resultDictionary = dict()
        resultDictionary["amountToAverageAmountRatioSince3Days"] = amountToAverageAmountRatioSince3Days
        resultDictionary["amountToAverageAmountDiffSince3Days"] = amountToAverageAmountDiffSince3Days
        resultDictionary["amountToMedianAmountRatioSince3Days"] = amountToMedianAmountRatioSince3Days
        resultDictionary["amountToMedianAmountDiffSince3Days"] = amountToMedianAmountDiffSince3Days
        resultDictionary[
            "amountMinusAverageAmountToDeviationAmountRatioSince3Days"] = amountMinusAverageAmountToDeviationAmountRatioSince3Days
        resultDictionary[
            "amountMinusAverageAmountToDeviationAmountDiffSince3Days"] = amountMinusAverageAmountToDeviationAmountDiffSince3Days

        resultDictionary["amountToAverageAmountRatioSince7Days"] = amountToAverageAmountRatioSince7Days
        resultDictionary["amountToAverageAmountDiffSince7Days"] = amountToAverageAmountDiffSince7Days
        resultDictionary["amountToMedianAmountRatioSince7Days"] = amountToMedianAmountRatioSince7Days
        resultDictionary["amountToMedianAmountDiffSince7Days"] = amountToMedianAmountDiffSince7Days
        resultDictionary[
            "amountMinusAverageAmountToDeviationAmountRatioSince7Days"] = amountMinusAverageAmountToDeviationAmountRatioSince7Days
        resultDictionary[
            "amountMinusAverageAmountToDeviationAmountDiffSince7Days"] = amountMinusAverageAmountToDeviationAmountDiffSince7Days

        resultDictionary["amountToAverageAmountRatioSince15Days"] = amountToAverageAmountRatioSince15Days
        resultDictionary["amountToAverageAmountDiffSince15Days"] = amountToAverageAmountDiffSince15Days
        resultDictionary["amountToMedianAmountRatioSince15Days"] = amountToMedianAmountRatioSince15Days
        resultDictionary["amountToMedianAmountDiffSince15Days"] = amountToMedianAmountDiffSince15Days
        resultDictionary[
            "amountMinusAverageAmountToDeviationAmountRatioSince15Days"] = amountMinusAverageAmountToDeviationAmountRatioSince15Days
        resultDictionary[
            "amountMinusAverageAmountToDeviationAmountDiffSince15Days"] = amountMinusAverageAmountToDeviationAmountDiffSince15Days

        resultDictionary["amountToAverageAmountRatioSince30Days"] = amountToAverageAmountRatioSince30Days
        resultDictionary["amountToAverageAmountDiffSince30Days"] = amountToAverageAmountDiffSince30Days
        resultDictionary["amountToMedianAmountRatioSince30Days"] = amountToMedianAmountRatioSince30Days
        resultDictionary["amountToMedianAmountDiffSince30Days"] = amountToMedianAmountDiffSince30Days
        resultDictionary[
            "amountMinusAverageAmountToDeviationAmountRatioSince30Days"] = amountMinusAverageAmountToDeviationAmountRatioSince30Days
        resultDictionary[
            "amountMinusAverageAmountToDeviationAmountDiffSince30Days"] = amountMinusAverageAmountToDeviationAmountDiffSince30Days

        resultDictionary["amountToAverageAmountRatioSinceFirstDay"] = amountToAverageAmountRatioSinceFirstDay
        resultDictionary["amountToAverageAmountDiffSinceFirstDay"] = amountToAverageAmountDiffSinceFirstDay
        resultDictionary["amountToMedianAmountRatioSinceFirstDay"] = amountToMedianAmountRatioSinceFirstDay
        resultDictionary["amountToMedianAmountDiffSinceFirstDay"] = amountToMedianAmountDiffSinceFirstDay
        resultDictionary[
            "amountMinusAverageAmountToDeviationAmountRatioSinceFirstDay"] = amountMinusAverageAmountToDeviationAmountRatioSinceFirstDay
        resultDictionary[
            "amountMinusAverageAmountToDeviationAmountDiffSinceFirstDay"] = amountMinusAverageAmountToDeviationAmountDiffSinceFirstDay
        return resultDictionary

    def get_transaction_number_properties(self, timestamp, amount_and_timestamp_collection):
        actual_date = int(timestamp)
        timestamp_collection = list()
        for item in amount_and_timestamp_collection:
            timestamp_collection.append(item[1])

        transactionDailyNumberCollectionSince3Days = list()
        transactionDailyNumberCollectionSince7Days = list()
        transactionDailyNumberCollectionSince15Days = list()
        transactionDailyNumberCollectionSince30Days = list()
        transactionDailyNumberCollectionSinceFirstDay = list()

        transactionNumberByDateDictionary = dict()
        transactionNumberByDateDictionary[actual_date] = 1
        for currentTimeStamp in timestamp_collection:
            currentDate = int(currentTimeStamp)
            if transactionNumberByDateDictionary.get(currentDate) is None:
                transactionNumberByDateDictionary[currentDate] = 1
            else:
                number = transactionNumberByDateDictionary.get(currentDate)
                number = number + 1
                transactionNumberByDateDictionary[currentDate] = number

        transactionNumberOnCurrentDay = transactionNumberByDateDictionary.get(actual_date)

        # if transactionNumberOnCurrentDay is None:
        #     print("transactionNumberOnCurrentDay NONE")
        #     transactionNumberOnCurrentDay = 1

        for date in transactionNumberByDateDictionary.keys():
            if date > timestamp - 3:
                transactionDailyNumberCollectionSince3Days.append(transactionNumberByDateDictionary.get(date))
            if date > timestamp - 7:
                transactionDailyNumberCollectionSince7Days.append(transactionNumberByDateDictionary.get(date))
            if date > timestamp - 15:
                transactionDailyNumberCollectionSince15Days.append(transactionNumberByDateDictionary.get(date))
            if date > timestamp - 30:
                transactionDailyNumberCollectionSince30Days.append(transactionNumberByDateDictionary.get(date))
            transactionDailyNumberCollectionSinceFirstDay.append(transactionNumberByDateDictionary.get(date))

        averageTransactionNumberSince3Days = statistics.mean(transactionDailyNumberCollectionSince3Days) if len(
            transactionDailyNumberCollectionSince3Days) > 0 else 0
        if averageTransactionNumberSince3Days is None:
            print("averageTransactionNumberSince3Days NONE")
        medianTransactionNumberSince3Days = statistics.median(transactionDailyNumberCollectionSince3Days) if len(
            transactionDailyNumberCollectionSince3Days) > 0 else 0
        transactionNumberToAverageDailyTransactionNumberRatioSince3Days = transactionNumberOnCurrentDay / averageTransactionNumberSince3Days if averageTransactionNumberSince3Days != 0 else 0
        transactionNumberToAverageDailyTransactionNumberDiffSince3Days = transactionNumberOnCurrentDay - averageTransactionNumberSince3Days
        transactionNumberToMedianDailyTransactionNumberRatioSince3Days = transactionNumberOnCurrentDay / medianTransactionNumberSince3Days if medianTransactionNumberSince3Days != 0 else 0
        transactionNumberToMedianDailyTransactionNumberDiffSince3Days = transactionNumberOnCurrentDay - medianTransactionNumberSince3Days

        averageTransactionNumberSince7Days = statistics.mean(transactionDailyNumberCollectionSince7Days) if len(
            transactionDailyNumberCollectionSince7Days) > 0 else 0
        medianTransactionNumberSince7Days = statistics.median(transactionDailyNumberCollectionSince7Days) if len(
            transactionDailyNumberCollectionSince7Days) > 0 else 0
        transactionNumberToAverageDailyTransactionNumberRatioSince7Days = transactionNumberOnCurrentDay / averageTransactionNumberSince7Days if averageTransactionNumberSince7Days != 0 else 0
        transactionNumberToAverageDailyTransactionNumberDiffSince7Days = transactionNumberOnCurrentDay - averageTransactionNumberSince7Days
        transactionNumberToMedianDailyTransactionNumberRatioSince7Days = transactionNumberOnCurrentDay / medianTransactionNumberSince7Days if medianTransactionNumberSince7Days != 0 else 0
        transactionNumberToMedianDailyTransactionNumberDiffSince7Days = transactionNumberOnCurrentDay - medianTransactionNumberSince7Days

        averageTransactionNumberSince15Days = statistics.mean(transactionDailyNumberCollectionSince15Days) if len(
            transactionDailyNumberCollectionSince15Days) > 0 else 0
        medianTransactionNumberSince15Days = statistics.median(transactionDailyNumberCollectionSince15Days) if len(
            transactionDailyNumberCollectionSince15Days) > 0 else 0
        transactionNumberToAverageDailyTransactionNumberRatioSince15Days = transactionNumberOnCurrentDay / averageTransactionNumberSince15Days if averageTransactionNumberSince15Days != 0 else 0
        transactionNumberToAverageDailyTransactionNumberDiffSince15Days = transactionNumberOnCurrentDay - averageTransactionNumberSince15Days
        transactionNumberToMedianDailyTransactionNumberRatioSince15Days = transactionNumberOnCurrentDay / medianTransactionNumberSince15Days if medianTransactionNumberSince15Days != 0 else 0
        transactionNumberToMedianDailyTransactionNumberDiffSince15Days = transactionNumberOnCurrentDay - medianTransactionNumberSince15Days

        averageTransactionNumberSince30Days = statistics.mean(transactionDailyNumberCollectionSince30Days) if len(
            transactionDailyNumberCollectionSince30Days) > 0 else 0
        medianTransactionNumberSince30Days = statistics.median(transactionDailyNumberCollectionSince30Days) if len(
            transactionDailyNumberCollectionSince30Days) > 0 else 0
        transactionNumberToAverageDailyTransactionNumberRatioSince30Days = transactionNumberOnCurrentDay / averageTransactionNumberSince30Days if averageTransactionNumberSince30Days != 0 else 0
        transactionNumberToAverageDailyTransactionNumberDiffSince30Days = transactionNumberOnCurrentDay - averageTransactionNumberSince30Days
        transactionNumberToMedianDailyTransactionNumberRatioSince30Days = transactionNumberOnCurrentDay / medianTransactionNumberSince30Days if medianTransactionNumberSince30Days != 0 else 0
        transactionNumberToMedianDailyTransactionNumberDiffSince30Days = transactionNumberOnCurrentDay - medianTransactionNumberSince30Days

        averageTransactionNumberSinceFirstDay = statistics.mean(transactionDailyNumberCollectionSinceFirstDay) if len(
            transactionDailyNumberCollectionSinceFirstDay) > 0 else 0
        medianTransactionNumberSinceFirstDay = statistics.median(transactionDailyNumberCollectionSinceFirstDay) if len(
            transactionDailyNumberCollectionSinceFirstDay) > 0 else 0
        transactionNumberToAverageDailyTransactionNumberRatioSinceFirstDay = transactionNumberOnCurrentDay / averageTransactionNumberSinceFirstDay if averageTransactionNumberSinceFirstDay != 0 else 0
        transactionNumberToAverageDailyTransactionNumberDiffSinceFirstDay = transactionNumberOnCurrentDay - averageTransactionNumberSinceFirstDay
        transactionNumberToMedianDailyTransactionNumberRatioSinceFirstDay = transactionNumberOnCurrentDay / medianTransactionNumberSinceFirstDay if medianTransactionNumberSinceFirstDay != 0 else 0
        transactionNumberToMedianDailyTransactionNumberDiffSinceFirstDay = transactionNumberOnCurrentDay - medianTransactionNumberSinceFirstDay

        resultDictionary = dict()
        resultDictionary[
            "transactionNumberToAverageDailyTransactionNumberRatioSince3Days"] = transactionNumberToAverageDailyTransactionNumberRatioSince3Days
        resultDictionary[
            "transactionNumberToAverageDailyTransactionNumberDiffSince3Days"] = transactionNumberToAverageDailyTransactionNumberDiffSince3Days
        resultDictionary[
            "transactionNumberToMedianDailyTransactionNumberRatioSince3Days"] = transactionNumberToMedianDailyTransactionNumberRatioSince3Days
        resultDictionary[
            "transactionNumberToMedianDailyTransactionNumberDiffSince3Days"] = transactionNumberToMedianDailyTransactionNumberDiffSince3Days

        resultDictionary[
            "transactionNumberToAverageDailyTransactionNumberRatioSince7Days"] = transactionNumberToAverageDailyTransactionNumberRatioSince7Days
        resultDictionary[
            "transactionNumberToAverageDailyTransactionNumberDiffSince7Days"] = transactionNumberToAverageDailyTransactionNumberDiffSince7Days
        resultDictionary[
            "transactionNumberToMedianDailyTransactionNumberRatioSince7Days"] = transactionNumberToMedianDailyTransactionNumberRatioSince7Days
        resultDictionary[
            "transactionNumberToMedianDailyTransactionNumberDiffSince7Days"] = transactionNumberToMedianDailyTransactionNumberDiffSince7Days

        resultDictionary[
            "transactionNumberToAverageDailyTransactionNumberRatioSince15Days"] = transactionNumberToAverageDailyTransactionNumberRatioSince15Days
        resultDictionary[
            "transactionNumberToAverageDailyTransactionNumberDiffSince15Days"] = transactionNumberToAverageDailyTransactionNumberDiffSince15Days
        resultDictionary[
            "transactionNumberToMedianDailyTransactionNumberRatioSince15Days"] = transactionNumberToMedianDailyTransactionNumberRatioSince15Days
        resultDictionary[
            "transactionNumberToMedianDailyTransactionNumberDiffSince15Days"] = transactionNumberToMedianDailyTransactionNumberDiffSince15Days

        resultDictionary[
            "transactionNumberToAverageDailyTransactionNumberRatioSince30Days"] = transactionNumberToAverageDailyTransactionNumberRatioSince30Days
        resultDictionary[
            "transactionNumberToAverageDailyTransactionNumberDiffSince30Days"] = transactionNumberToAverageDailyTransactionNumberDiffSince30Days
        resultDictionary[
            "transactionNumberToMedianDailyTransactionNumberRatioSince30Days"] = transactionNumberToMedianDailyTransactionNumberRatioSince30Days
        resultDictionary[
            "transactionNumberToMedianDailyTransactionNumberDiffSince30Days"] = transactionNumberToMedianDailyTransactionNumberDiffSince30Days

        resultDictionary[
            "transactionNumberToAverageDailyTransactionNumberRatioSinceFirstDay"] = transactionNumberToAverageDailyTransactionNumberRatioSinceFirstDay
        resultDictionary[
            "transactionNumberToAverageDailyTransactionNumberDiffSinceFirstDay"] = transactionNumberToAverageDailyTransactionNumberDiffSinceFirstDay
        resultDictionary[
            "transactionNumberToMedianDailyTransactionNumberRatioSinceFirstDay"] = transactionNumberToMedianDailyTransactionNumberRatioSinceFirstDay
        resultDictionary[
            "transactionNumberToMedianDailyTransactionNumberDiffSinceFirstDay"] = transactionNumberToMedianDailyTransactionNumberDiffSinceFirstDay
        return resultDictionary

    def get_transaction_interval_properties(self, timestamp, amount_and_timestamp_collection):
        timestamp_collection = list()
        for item in amount_and_timestamp_collection:
            timestamp_collection.append(item[1])
        length_of_timestamp_collection = len(timestamp_collection)
        current_interval = timestamp - timestamp_collection[0] if length_of_timestamp_collection >= 1 else 0
        intervals_since3_days = list()
        intervals_since7_days = list()
        intervals_since15_days = list()
        intervals_since30_days = list()
        intervals_since_first_day = list()
        for i in range(0, length_of_timestamp_collection - 1, 1):
            nextTimeStamp = timestamp_collection[i + 1]
            currentTimeStamp = timestamp_collection[i]
            interval = currentTimeStamp - nextTimeStamp
            if currentTimeStamp > timestamp - 3:
                intervals_since3_days.append(interval)
            if currentTimeStamp > timestamp - 7:
                intervals_since7_days.append(interval)
            if currentTimeStamp > timestamp - 15:
                intervals_since15_days.append(interval)
            if currentTimeStamp > timestamp - 30:
                intervals_since30_days.append(interval)
            intervals_since_first_day.append(interval)

        averageIntervalSince3Days = statistics.mean(intervals_since3_days) if len(intervals_since3_days) > 0 else 0
        medianIntervalSince3Days = statistics.median(intervals_since3_days) if len(intervals_since3_days) > 0 else 0
        averageIntervalSince7Days = statistics.mean(intervals_since7_days) if len(intervals_since7_days) > 0 else 0
        medianIntervalSince7Days = statistics.median(intervals_since7_days) if len(intervals_since7_days) > 0 else 0
        averageIntervalSince15Days = statistics.mean(intervals_since15_days) if len(intervals_since15_days) > 0 else 0
        medianIntervalSince15Days = statistics.median(intervals_since15_days) if len(intervals_since15_days) > 0 else 0
        averageIntervalSince30Days = statistics.mean(intervals_since30_days) if len(intervals_since30_days) > 0 else 0
        medianIntervalSince30Days = statistics.median(intervals_since30_days) if len(intervals_since30_days) > 0 else 0
        averageIntervalSinceFirstDay = statistics.mean(intervals_since_first_day) if len(
            intervals_since_first_day) > 0 else 0
        medianIntervalSinceFirstDay = statistics.median(intervals_since_first_day) if len(
            intervals_since_first_day) > 0 else 0

        intervalToAverageIntervalRatioSince3Days = current_interval / averageIntervalSince3Days if averageIntervalSince3Days != 0 else 0
        intervalToAverageIntervalDiffSince3Days = current_interval - averageIntervalSince3Days
        intervalToMedianIntervalRatioSince3Days = current_interval / medianIntervalSince3Days if medianIntervalSince3Days != 0 else 0
        intervalToMedianIntervalDiffSince3Days = current_interval - medianIntervalSince3Days
        intervalToAverageIntervalRatioSince7Days = current_interval / averageIntervalSince7Days if averageIntervalSince7Days != 0 else 0
        intervalToAverageIntervalDiffSince7Days = current_interval - averageIntervalSince7Days
        intervalToMedianIntervalRatioSince7Days = current_interval / medianIntervalSince7Days if medianIntervalSince7Days != 0 else 0
        intervalToMedianIntervalDiffSince7Days = current_interval - medianIntervalSince7Days
        intervalToAverageIntervalRatioSince15Days = current_interval / averageIntervalSince15Days if averageIntervalSince15Days != 0 else 0
        intervalToAverageIntervalDiffSince15Days = current_interval - averageIntervalSince15Days
        intervalToMedianIntervalRatioSince15Days = current_interval / medianIntervalSince15Days if medianIntervalSince15Days != 0 else 0
        intervalToMedianIntervalDiffSince15Days = current_interval - medianIntervalSince15Days
        intervalToAverageIntervalRatioSince30Days = current_interval / averageIntervalSince30Days if averageIntervalSince30Days != 0 else 0
        intervalToAverageIntervalDiffSince30Days = current_interval - averageIntervalSince30Days
        intervalToMedianIntervalRatioSince30Days = current_interval / medianIntervalSince30Days if medianIntervalSince30Days != 0 else 0
        intervalToMedianIntervalDiffSince30Days = current_interval - medianIntervalSince30Days
        intervalToAverageIntervalRatioSinceFirstDay = current_interval / averageIntervalSinceFirstDay if averageIntervalSinceFirstDay != 0 else 0
        intervalToAverageIntervalDiffSinceFirstDay = current_interval - averageIntervalSinceFirstDay
        intervalToMedianIntervalRatioSinceFirstDay = current_interval / medianIntervalSinceFirstDay if medianIntervalSinceFirstDay != 0 else 0
        intervalToMedianIntervalDiffSinceFirstDay = current_interval - medianIntervalSinceFirstDay

        resultDictionary = dict()
        resultDictionary["intervalToAverageIntervalRatioSince3Days"] = intervalToAverageIntervalRatioSince3Days
        resultDictionary["intervalToAverageIntervalDiffSince3Days"] = intervalToAverageIntervalDiffSince3Days
        resultDictionary["intervalToMedianIntervalRatioSince3Days"] = intervalToMedianIntervalRatioSince3Days
        resultDictionary["intervalToMedianIntervalDiffSince3Days"] = intervalToMedianIntervalDiffSince3Days

        resultDictionary["intervalToAverageIntervalRatioSince7Days"] = intervalToAverageIntervalRatioSince7Days
        resultDictionary["intervalToAverageIntervalDiffSince7Days"] = intervalToAverageIntervalDiffSince7Days
        resultDictionary["intervalToMedianIntervalRatioSince7Days"] = intervalToMedianIntervalRatioSince7Days
        resultDictionary["intervalToMedianIntervalDiffSince7Days"] = intervalToMedianIntervalDiffSince7Days

        resultDictionary["intervalToAverageIntervalRatioSince15Days"] = intervalToAverageIntervalRatioSince15Days
        resultDictionary["intervalToAverageIntervalDiffSince15Days"] = intervalToAverageIntervalDiffSince15Days
        resultDictionary["intervalToMedianIntervalRatioSince15Days"] = intervalToMedianIntervalRatioSince15Days
        resultDictionary["intervalToMedianIntervalDiffSince15Days"] = intervalToMedianIntervalDiffSince15Days

        resultDictionary["intervalToAverageIntervalRatioSince30Days"] = intervalToAverageIntervalRatioSince30Days
        resultDictionary["intervalToAverageIntervalDiffSince30Days"] = intervalToAverageIntervalDiffSince30Days
        resultDictionary["intervalToMedianIntervalRatioSince30Days"] = intervalToMedianIntervalRatioSince30Days
        resultDictionary["intervalToMedianIntervalDiffSince30Days"] = intervalToMedianIntervalDiffSince30Days

        resultDictionary["intervalToAverageIntervalRatioSinceFirstDay"] = intervalToAverageIntervalRatioSinceFirstDay
        resultDictionary["intervalToAverageIntervalDiffSinceFirstDay"] = intervalToAverageIntervalDiffSinceFirstDay
        resultDictionary["intervalToMedianIntervalRatioSinceFirstDay"] = intervalToMedianIntervalRatioSinceFirstDay
        resultDictionary["intervalToMedianIntervalDiffSinceFirstDay"] = intervalToMedianIntervalDiffSinceFirstDay

        resultDictionary["time_stamp_difference"] = current_interval
        return resultDictionary
