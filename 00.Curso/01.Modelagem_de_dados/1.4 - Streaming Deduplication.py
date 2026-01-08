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
# MAGIC Identificando quantidade de dados na bronze para tabela orders

# COMMAND ----------

(spark.read
      .table("bronze")
      .filter("topic = 'orders'")
      .count()
)

# COMMAND ----------

# MAGIC %md
# MAGIC Excluindo linhas duplicadas

# COMMAND ----------

from pyspark.sql import functions as F

json_schema = "order_id STRING, order_timestamp Timestamp, customer_id STRING, quantity BIGINT, total BIGINT, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>"

batch_total = (spark.read
                      .table("bronze")
                      .filter("topic = 'orders'")
                      .select(F.from_json(F.col("value").cast("string"), json_schema).alias("v"))
                      .select("v.*")
                      .dropDuplicates(["order_id", "order_timestamp"]) #Excluindo valores duplicados quando as colunas order_id e order_timestamp são duplicadas
                      .count()
                )

print(batch_total)

# COMMAND ----------

# MAGIC %md
# MAGIC também é possivel utilizar a função quando utilizamos readStream

# COMMAND ----------

deduped_df = (spark.readStream
                   .table("bronze")
                   .filter("topic = 'orders'")
                   .select(F.from_json(F.col("value").cast("string"), json_schema).alias("v"))
                   .select("v.*")
                   .withWatermark("order_timestamp", "30 seconds") #Restringe o tempo observado em relação ao tempo de execução. Sem ele pode levar a problemas de memoria
                   .dropDuplicates(["order_id", "order_timestamp"]))

# COMMAND ----------

# MAGIC %md
# MAGIC Definindo uma Função específica para criar uma tabela temporária que servirá de comparação entre o dado existente com o novo dado de entrada e adicionar os arquivos utilizando merge, garantindo que não tenha duplicidade

# COMMAND ----------

def upsert_data(microBatchDF, batch):
    microBatchDF.createOrReplaceTempView("orders_microbatch")
    
    sql_query = """
      MERGE INTO orders_silver a
      USING orders_microbatch b
      ON a.order_id=b.order_id AND a.order_timestamp=b.order_timestamp
      WHEN NOT MATCHED THEN INSERT *
    """
    
    microBatchDF.sparkSession.sql(sql_query) # funciona apenas com versão do Runtime acima da 10.5
    #microBatchDF._jdf.sparkSession().sql(sql_query) ----> Utilizado para versões abaixo da 10.5

# COMMAND ----------

# MAGIC %md
# MAGIC Criando a tabela caso não exista antes de salvar

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS orders_silver
# MAGIC (order_id STRING, order_timestamp Timestamp, customer_id STRING, quantity BIGINT, total BIGINT, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>)

# COMMAND ----------

# MAGIC %md
# MAGIC Salvando os arquivos com execução em batch

# COMMAND ----------

query = (deduped_df.writeStream
                   .foreachBatch(upsert_data) #Adiciona uma função de lógica específica para cada micro-batch
                   .option("checkpointLocation", f"{bookstore.checkpoint_path}/orders_silver")
                   .trigger(availableNow=True)
                   .start())

query.awaitTermination()

# COMMAND ----------

streaming_total = spark.read.table("orders_silver").count()

print(f"batch total: {batch_total}")
print(f"streaming total: {streaming_total}")
