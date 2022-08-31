# airflow-examples


Exemplos de utilização de Operators do Airflow.


- [s3_transform_dag](/source/dags/dags/s3_transform_dag.py): exemplo com a utilização do 
S3FileTransformOperator. Os dados são carregados do S3, realizada a transformação (por um script)
e armazenado o resultado no S3. [Mais detalhes na documentação](/source/dags/etl/s3_file_transform/README.md).