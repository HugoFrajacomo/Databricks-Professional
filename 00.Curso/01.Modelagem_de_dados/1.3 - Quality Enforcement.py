# Databricks notebook source
# MAGIC %run ../00.Includes/Copy-Datasets

# COMMAND ----------

# MAGIC %md
# MAGIC ## Definindo restrições (CONSTRAINT) em tabelas existentes

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE orders_silver ADD CONSTRAINT timestamp_within_range CHECK (order_timestamp >= '2020-01-01'); -- verifica se o order_timestamp é maior ou igual a 2020-01-01

# COMMAND ----------

# MAGIC %md
# MAGIC É possivel visualizar as restrições existentes na tabela executando o comando DESCRIBE EXTENDED nome_tabela. Ele irá mostrar em Table Properties

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED orders_silver

# COMMAND ----------

# MAGIC %md
# MAGIC ## Simulando inserção de carga com violação

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO orders_silver
# MAGIC VALUES ('1', '2022-02-01 00:00:00.000', 'C00001', 0, 0, NULL),
# MAGIC        ('2', '2019-05-01 00:00:00.000', 'C00001', 0, 0, NULL),
# MAGIC        ('3', '2023-01-01 00:00:00.000', 'C00001', 0, 0, NULL)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM orders_silver
# MAGIC WHERE order_id IN ('1', '2', '3')

# COMMAND ----------

# MAGIC %md
# MAGIC Como visto a falha não permitiu a incersão dos dados

# COMMAND ----------

# MAGIC %md
# MAGIC ## Adicioando uma nova constrant
# MAGIC
# MAGIC Vamos simular adicionar uma regra a uma tabela existente em que os dados pré-existentes não cumpreem os requisitos da restição a ser adicionada

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE orders_silver ADD CONSTRAINT valid_quantity CHECK (quantity > 0); -- Verifica se quantity é maior que 0

# COMMAND ----------

# MAGIC %md
# MAGIC Ela falha avisando que 24 linhas violam a restrição e ela não é adicionada a tabela

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED orders_silver

# COMMAND ----------

# MAGIC %md
# MAGIC Verificando arquivos que não comprem a restrição que queremos adicionar

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM orders_silver
# MAGIC where quantity <= 0

# COMMAND ----------

# MAGIC %md
# MAGIC Possivel solução para o erro: Alterar a ingestão de dados da bronze para silver adicionado um filtro que satisfaça a constraint para depois aplicarmos a regra de constraint na tabela já com os dados que davam problema excluidos

# COMMAND ----------

from pyspark.sql import functions as F

json_schema = "order_id STRING, order_timestamp Timestamp, customer_id STRING, quantity BIGINT, total BIGINT, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>"

query = (spark.readStream.table("bronze")
        .filter("topic = 'orders'")
        .select(F.from_json(F.col("value").cast("string"), json_schema).alias("v"))
        .select("v.*")
        .filter("quantity > 0") #Linha adicionada para filtrar a quantidade
     .writeStream
        .option("checkpointLocation", f"{bookstore.checkpoint_path}/orders_silver")
        .trigger(availableNow=True)
        .table("orders_silver"))

query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Excluindo Restrições

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE orders_silver DROP CONSTRAINT timestamp_within_range;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED orders_silver

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resetando a tabela para trabalhar em outros notebooks

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE orders_silver

# COMMAND ----------

dbutils.fs.rm(f"{bookstore.checkpoint_path}/orders_silver", True)
