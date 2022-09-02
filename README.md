# airflow-examples

Alguns códigos de exemplo para pipelines no Airflow.

## Índice

- [s3_transform_dag](#s3_transform_dag)
- [s3_to_s3_transform_dag](#s3_to_s3_transform_dag)


### s3_transform_dag 

A DAG [s3_transform_dag](/source/dags/dags/s3_transform_dag.py) exemplifica a utilização do **S3FileTransformOperator**. 

Este operator carrega o arquivo do S3, posteriormente, encaminha o arquivo para um script que realiza o processo
de transformação, esse script pode ser em Python ou em qualquer outra linguagem. Após a transformação os dados
são enviados novamente para o S3.

Mais detalhes do pipeline pode ser encontrada na [documentação](/docs/S3FileTransformOperator.md).


### s3_to_s3_transform_dag

A DAG [s3_to_s3_transform_dag](/source/dags/dags/s3_to_s3_transform_dag.py) utiliza o operator **S3PythonTransformOperator**
para realizar a mesma operação que a **S3FileTransformOperator**.

Os dois operators tem o mesmo objetivo, carregar um arquivo do S3, realizar uma transformação e despejar o resultado
em outro arquivo no S3. A diferença está no processo de transformação. O **S3FileTransformOperator** utiliza uma função
**Callable**, idêntico ao **PythonOperator**, e não um arquivo de script.
