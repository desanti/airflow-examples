import os
import unittest

from airflow import AirflowException

from etl.preprocess.config import Config


class TestReadConfig(unittest.TestCase):
    def setUp(self) -> None:
        self.config = Config()
        self.config.path = os.path.dirname(os.path.abspath(__file__))

    def test_success_config_without_template(self):
        company_config = self.config.get_config("company_1")

        self.assertIsNotNone(company_config)
        self.assertIsInstance(company_config, dict)
        self.assertIn("column", company_config.keys())

    def test_success_config_with_template(self):
        company_config = self.config.get_config("company_2")

        self.assertIsNotNone(company_config)
        self.assertIsInstance(company_config, dict)
        self.assertIn("column", company_config.keys())

    def test_error_company_doesnt_exists(self):
        with self.assertRaises(AirflowException) as error:
            company_config = self.config.get_config("company_4")

        self.assertEqual(str(error.exception), "Company doesn't have configuration.")

    def test_error_template_doesnt_exist(self):
        with self.assertRaises(AirflowException) as error:
            company_config = self.config.get_config("company_3")

        self.assertEqual(
            str(error.exception), "Company doesn't have template configuration."
        )
