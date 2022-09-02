"""

Exemplo de utilização do **S3FileTransformOperator**.

Onde carrega o arquivo **ds_salaries.csv** do S3, e armazena o arquivo **data_engineer_salaries** em formato **PARQUET**
após algumas transformações.

"""


from datetime import datetime

from airflow import DAG

from config.datalake_zones import RAW_ZONE, TRUSTED_ZONE
from etl.s3_file_transform.data_cleaning import data_cleaning
from operators.s3_python_transform import S3PythonTransformOperator

with DAG(
    "s3_to_s3_transform_dag",
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

    def transform(job_title_filter, input_filename, output_filename, **kwargs):
        dag.log.info("Transforming...")

        data_cleaning(input_filename, output_filename, job_title_filter)

    s3_transform = S3PythonTransformOperator(
        task_id="s3_transform",
        source_s3_bucket=RAW_ZONE,
        source_s3_key="salaries/ds_salaries.csv",
        dest_s3_bucket=TRUSTED_ZONE,
        dest_s3_key="salaries/data_engineer_salaries_v2.parquet",
        replace=True,
        python_callable=transform,
        # op_args=["Data Engineer"],
        op_kwargs={"job_title_filter": "Data Engineer"},
    )
