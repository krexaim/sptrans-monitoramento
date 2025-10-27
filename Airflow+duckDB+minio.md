Fluxo recomendado (prático para Airflow + NRT)

PySpark (task Airflow) — transforma e grava Parquet particionado em MinIO (S3 path).

DuckDB (task Airflow PythonOperator) — conecta ao DuckDB e executa CREATE TABLE AS SELECT * FROM read_parquet('s3://.../*.parquet') ou directly query the parquet files.

(Opcional) Persistir DuckDB .db ou manter DuckDB em memória para consultas ad-hoc. Se precisar de persistência compartilhada, prefira exportar tabelas/Parquet curados para um formato acessível pelos consumidores.

Considerações de performance e NRT

Latência: DuckDB lendo muitos pequenos ficheiros Parquet(chunks) em S3 pode gerar muitas requisições HTTP (overhead). Para NRT, prefira fewer larger files ou particionamento bem pensado. Monitorar número de requests ao MinIO. 
Server Fault

Escala: DuckDB é excepcional para consultas analíticas locais e cargas até centenas de GB em uma máquina bem provisionada; para petabytes/alto paralelismo, Spark/cluster é melhor. Avalie o tamanho do trusted dataset que DuckDB terá que servir. 
Miles Cole

Consistência: assegure que Spark finalize e escreva atomically os arquivos (por exemplo, escrever em path tmp e renomear/atomizar) antes do job DuckDB ler.

Conclusão / Recomendação

Para um pipeline PySpark + Airflow em ambiente NRT + MinIO:

Escreva Parquet partionado a partir do PySpark para MinIO (trusted/silver).

Use DuckDB (httpfs) para ler esses Parquets e materializar tabelas ou criar visões curadas.

Orquestre no Airflow com tarefas separadas (Spark → DuckDB).

Esse padrão é robusto, evita problemas de concorrência de escrita no DuckDB e alinha bem com a arquitetura medalhão. 
DuckDB
+2
DuckDB
+2


├── dags/
│   └── trusted_load.py           # DAG principal que orquestra a execução
│
├── scripts/
│   ├── pyspark-parquet.py        # Script que cria Parquet de teste e envia para MinIO
│   └── duckdb-minio.py           # Script que lê o Parquet do MinIO e cria tabela DuckDB
│
