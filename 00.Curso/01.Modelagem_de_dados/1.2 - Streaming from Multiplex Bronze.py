# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Professional/main/Includes/images/orders.png" width="60%">
# MAGIC </div>

# COMMAND ----------

# MAGIC %run ../00.Includes/Copy-Datasets

# COMMAND ----------

# MAGIC %md
# MAGIC ## Criando as tabelas
# MAGIC
# MAGIC 1. Verificar Chave valor das respectivas tabelas

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT cast(key AS STRING), cast(value AS STRING)
# MAGIC FROM bronze
# MAGIC LIMIT 20

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tabela orders

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Selecinando dados de orders da tabela geral

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT v.*
# MAGIC FROM (
# MAGIC   SELECT from_json(cast(value AS STRING), "order_id STRING, order_timestamp Timestamp, customer_id STRING, quantity BIGINT, total BIGINT, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>") v
# MAGIC   FROM bronze
# MAGIC   WHERE topic = "orders")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Orders Temporária

# COMMAND ----------

# MAGIC %md
# MAGIC Criação da tabela bronze_temp

# COMMAND ----------

(spark.readStream
      .table("bronze")
      .createOrReplaceTempView("bronze_tmp"))

# COMMAND ----------

# MAGIC %md
# MAGIC Adicionando dados na tabela (Semente os dados referente ao "topic" = "orders")

# COMMAND ----------

# Defina explicitamente o local do checkpoint para a exibição do streaming, pois locais temporários implícitos de checkpoint não são suportados no workspace atual

orders_silver_df = spark.sql("""
    SELECT v.*
    FROM (
    SELECT from_json(cast(value AS STRING), "order_id STRING, order_timestamp Timestamp, customer_id STRING, quantity BIGINT, total BIGINT, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>") v
    FROM bronze_tmp
    WHERE topic = "orders")
""")

display(orders_silver_df, checkpointLocation = f"{bookstore.checkpoint_path}/tmp/orders_silver_{time.time()}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Salvando a tabela orders na camada bronze

# COMMAND ----------

# MAGIC %md
# MAGIC Criando uma tempview para silver para possibilitar trabalhar com spark df

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW orders_silver_tmp AS
# MAGIC   SELECT v.*
# MAGIC   FROM (
# MAGIC     SELECT from_json(cast(value AS STRING), "order_id STRING, order_timestamp Timestamp, customer_id STRING, quantity BIGINT, total BIGINT, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>") v
# MAGIC     FROM bronze_tmp
# MAGIC     WHERE topic = "orders")

# COMMAND ----------

# MAGIC %md
# MAGIC Gravando os dados da tabela temporária e salvando na silver layer

# COMMAND ----------

query = (spark.table("orders_silver_tmp")
               .writeStream
               .option("checkpointLocation", f"{bookstore.checkpoint_path}/orders_silver")
               .trigger(availableNow=True)
               .table("orders_silver"))

query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC Código para realizar o mesmo processo utilizando spark sql

# COMMAND ----------

from pyspark.sql import functions as F

json_schema = "order_id STRING, order_timestamp Timestamp, customer_id STRING, quantity BIGINT, total BIGINT, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>"

query = (spark.readStream.table("bronze")
        .filter("topic = 'orders'")
        .select(F.from_json(F.col("value").cast("string"), json_schema).alias("v"))
        .select("v.*")
     .writeStream
        .option("checkpointLocation", f"{bookstore.checkpoint_path}/orders_silver")
        .trigger(availableNow=True)
        .table("orders_silver"))

query.awaitTermination()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM orders_silver
