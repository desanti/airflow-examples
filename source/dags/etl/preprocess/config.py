import os

import yaml
from airflow import AirflowException


class Config:
    def __init__(self):
        self.path = os.path.dirname(os.path.abspath(__file__))

    def get_config(self, company_id):
        configs = self._get_configs()

        company_config = configs.get(company_id)
        if not company_config:
            raise AirflowException("Company doesn't have configuration.")

        if not company_config.get("template"):
            return company_config

        template_config = self._get_template(company_config.get("template"))
        company_config = self._join_template(company_config, template_config)
        return company_config

    def _get_configs(self) -> dict:
        config_file = os.path.join(self.path, "config.yaml")

        with open(config_file) as file:
            configs = yaml.full_load(file)

        return configs

    def _get_template(self, template_name: str) -> dict:
        template_file = os.path.join(self.path, "template.yaml")
        with open(template_file) as file:
            templates = yaml.full_load(file)

        template = templates.get(template_name)
        if not template:
            raise AirflowException("Company doesn't have template configuration.")

        return template

    @staticmethod
    def _join_template(company_config, template_config):
        company_config.update(template_config)
        return company_config
