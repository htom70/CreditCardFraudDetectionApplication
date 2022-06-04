import encoding_module
import logging

import unittest

encoding_parametrs = {
    "card_number" : "label_encoder",
    "transaction_type": "omit"
}

detailed_information_about_table = {
    "fields": [
        {
            "name": "id",
            "other_eligible_type_for_encoding": [],
            "type": "bigint"
        },
        {
            "name": "card_number",
            "other_eligible_type_for_encoding": [
                "float"
            ],
            "type": "varchar"
        },
        {
            "name": "transaction_type",
            "other_eligible_type_for_encoding": [],
            "type": "int"
        },
        {
            "name": "timestamp",
            "other_eligible_type_for_encoding": [],
            "type": "datetime"
        },
        {
            "name": "amount",
            "other_eligible_type_for_encoding": [],
            "type": "int"
        },
        {
            "name": "currency_name",
            "other_eligible_type_for_encoding": [],
            "type": "varchar"
        },
        {
            "name": "response_code",
            "other_eligible_type_for_encoding": [],
            "type": "int"
        },
        {
            "name": "country_name",
            "other_eligible_type_for_encoding": [],
            "type": "varchar"
        },
        {
            "name": "vendor_code",
            "other_eligible_type_for_encoding": [
                "float",
                "int"
            ],
            "type": "varchar"
        },
        {
            "name": "fraud",
            "other_eligible_type_for_encoding": [],
            "type": "int"
        }
    ],
    "fraud_candidates": [
        {
            "fraud_number": 601,
            "name": "fraud",
            "no_fraud_number": 11314
        }
    ],
    "primary_key": [
        "id"
    ],
    "record_number": 11915
}


class TestEncoding(unittest.TestCase):
    def test_create_modified_fields(self):
        original_fields = ["id", "card_number", "transaction_type", "timestamp", "amount", "currency_name",
                           "response_code", "country_name", "vendor_code", "fraud"]
        database_encoder = encoding_module.DataBaseEncoder(None, original_fields)
        modified_fields = database_encoder.create_modified_fields(encoding_parametrs, "id")
        self.assertEqual(1, len(modified_fields))

    def test_get_field_type_before_encoding(self):
        original_fields = ["card_number", "transaction_type"]
        database_encoder = encoding_module.DataBaseEncoder(None, original_fields)
        field_type = database_encoder.get_field_type_before_encoding("vendor_code", detailed_information_about_table)
        self.assertEqual("varchar", field_type)

if __name__ == '__main__':
    unittest.main()