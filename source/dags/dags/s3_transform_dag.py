"""

Exemplo de utilização do **S3FileTransformOperator**.

Onde carrega o arquivo **ds_salaries.csv** do S3, e armazena o arquivo **data_engineer_salaries** em formato **PARQUET**
após algumas transformações.

Mais detalhes na [documentação]()

"""


from datetime import datetime

from airflow import DAG
from airflow.providers.amazon.aws.operators.s3 import S3FileTransformOperator

from config.datalake_zones import RAW_ZONE, TRUSTED_ZONE

with DAG(
    "s3_transform_dag",
    default_args={
        "depends_on_past": False,
        "owner": "Data",
    },
    schedule_interval=None,
    start_date=datetime(2022, 8, 1),
    catchup=False,
    tags=["s3", "transform", "example"],
    description="Transforma o dataset salaries em CSV para PARQUET. ",
) as dag:
    dag.doc_md = __doc__

    s3_transform = S3FileTransformOperator(
        task_id="s3_transform",
        source_s3_key=f"s3://{RAW_ZONE}/salaries/ds_salaries.csv",
        dest_s3_key=f"s3://{TRUSTED_ZONE}/salaries/data_engineer_salaries.parquet",
        replace=True,
        transform_script="source/dags/etl/s3_file_transform/transform.py",
        script_args=["Data Engineer"],
    )
