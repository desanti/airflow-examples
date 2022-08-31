# Exemplo de utilização do S3FileTransformOperator 

O objetivo desse pipeline é disponibilizar um arquivo PARQUET com os salários dos "Engenheiro de Dados".

Para isso foi utilizado o dataset [`data-science-job-salaries`](https://www.kaggle.com/datasets/ruchi798/data-science-job-salaries?resource=download) 
disponibilizado no Kaggle. Nele contem informações de vários profissionais da área de dados, incluindo, 
Engenheiro de Dados, Cientista de Dados entre outros.

Para esse processo foi utilizado o `S3FileTransformOperator` para as operações com o `S3`, 
onde está o dataset `ds_salaries.csv`, e onde será armazenado o PARQUET `data_engineer_salaries.parquet`. 
Para a manipulação dos dados foi utilizado o `pandas`.

O `S3FileTransformOperator` carrega um arquivo do `S3`, realiza a transformação através de um script, 
e então armazena o resultado no `S3` novamente. O script não é obrigatório que seja em `python`.

Este é um processo simples utilizando o `S3FileTransformOperator`. 
O repositório do [jamesang17](https://github.com/jamesang17/airflow-app) tem um pipeline mais completo utilizando esse operator.


### S3_TRANSFORM_DAG.PY

Na [DAG](../../dags/s3_transform_dag.py) apresenta a utilização do `S3FileTransformOperator`. 
Abaixo o código e a explicação dos principais parâmetros.


```python
s3_transform = S3FileTransformOperator(
    task_id="s3_transform",
    source_s3_key=f"s3://{RAW_ZONE}/salaries/ds_salaries.csv",
    dest_s3_key=f"s3://{TRUSTED_ZONE}/salaries/data_engineer_salaries.parquet",
    replace=True,
    transform_script="source/dags/etl/s3_file_transform/transform.py",
    script_args=["Data Engineer"],
)
```

Parâmetros:

- `source_s3_key`: arquivo com os dados de entrada, localizado no `S3`. Deve passar o caminho completo, 
inclusive com o prefixo `s3://`.
- `dest_s3_key`: arquivo que conterá os dados de saída, após a transformação.
- `transform_script`: caminho para o script de transformação dos dados. O caminho pode mudar conforme a estrutura 
de pastas do projeto. 
- `script_args`: lista de parâmetros utilizado pelo script de transformação. Neste exemplo foi passado como parâmetro 
a palavra "Data Engineer", que será utilizada como filtro no dataset. 


### TRANSFORM.PY

O [`transform.py`](transform.py) é o `entrypoint` para realizar as transformações necessárias no dataset. 
Alguns pontos importantes são explicados a seguir.

#### IDENTIFICAÇÃO DO INTERPRETADOR DE SCRIPT

A linha abaixo é necessária para que o sistema operacional saiba qual interpretador de script deverá ser utilizado.
Caso não tenha essa informação, o script não será executado, retornando erro para o Airflow.

```python
#!/usr/bin/env python3
```

Nesse caso foi utilizado o `python`, mas pode ser utilizado qualquer outro de script, como por exemplo, `bash`.


#### IMPORT DO PYTHON

Agora sobre o `import` dos módulos próprios em `python`, o caminho deve ser a partir do `transform.py`, 
e não a partir do `Source Root`, que neste caso é o `/source/dags`.


```python
from data_cleaning import data_cleaning
```

#### ARGUMENTOS DE ENTRADA DO SCRIPT

O `S3FileTransformOperator` executa o script em um subprocesso, passando alguns argumentos. 
Esses argumentos podem ser acessados utilizando o `sys.argv`, conforme pode ser observado no código abaixo:

```python
arg_list = ["script", "input", "output", "job_title_filter"]
args = dict(zip(arg_list, sys.argv))
```

Os argumentos são:

- `script`: nome do arquivo sendo executado, nesse caso: `transform.py`.
- `input`: nome do arquivo temporário com os dados baixados do `S3`.
- `output`: nome do arquivo temporário que será enviado para o `S3` após o processo de transformação. 
É nesse arquivo que os dados devem ser armazenados. 
- `script_args`: informações passadas no parâmetro `script_args` do `S3FileTransformOperator`. 
Pode ser passado uma lista de informações. Neste exemplo, foi passado apenas um parâmetro, mapeado para `job_title_filter`.

#### PROCESSAMENTO DOS DADOS

O processamento dos dados foi escrito no módulo `data_cleaning.py`, explanado no próximo tópico.

```python
logging.info("Starting data cleaning...")

data_cleaning(args.get("input"), args.get("output"), args.get("job_title_filter"))

logging.info("Completed data cleaning!")
```


### DATA_CLEANING.PY

Nesse [arquivo](data_cleaning.py) está o código para tratamento dos dados.

Dois pontos devem ser observados. O primeiro é o parâmetro `input_file`, que é o nome do arquivo temporário com os dados.
Ele pode ser lido normalmente pelo `pandas`. Conforme o código abaixo:

```python
df = pd.read_csv(input_file)
```

O segundo ponto é a forma de armazenar o resultado da transformação. O `S3FileTransformOperator` também passa o nome do
arquivo temporário (armazenado na variável `output_file`) que deverá ser gravado o resultado. Então, basta utilizar esse parâmetro nos métodos do `pandas`, 
conforme pode ser observado no código abaixo:


```python
df.to_parquet(output_file, index=False)
```


## Links:

- Documentação para o S3FileTransformOperator: https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/operators/s3.html#transform-an-amazon-s3-object
- Dataset: https://www.kaggle.com/datasets/ruchi798/data-science-job-salaries?resource=download
- Repositório jamesang17: https://github.com/jamesang17/airflow-app