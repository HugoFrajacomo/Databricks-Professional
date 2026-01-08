# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Professional/main/Includes/images/customers.png" width="60%">
# MAGIC </div>

# COMMAND ----------

# MAGIC %run ../00.Includes/Copy-Datasets

# COMMAND ----------

from pyspark.sql import functions as F

schema = "customer_id STRING, email STRING, first_name STRING, last_name STRING, gender STRING, street STRING, city STRING, country_code STRING, row_status STRING, row_time timestamp"

customers_df = (spark.table("bronze")
                 .filter("topic = 'customers'") #Filtando apenas os clientes
                 .select(F.from_json(F.col("value").cast("string"), schema).alias("v")) #Formatando para o schema correto
                 .select("v.*")
                 .filter(F.col("row_status").isin(["insert", "update"]))) #Será inserido apenas os inserts e updates

display(customers_df)

# COMMAND ----------

from pyspark.sql.window import Window

window = Window.partitionBy("customer_id").orderBy(F.col("row_time").desc())

ranked_df = (customers_df.withColumn("rank", F.rank().over(window)) #Aplicando a função rank
                          .filter("rank == 1") #Mantem apenas o ultimo registro atualizado
                          .drop("rank")) #Deleta a coluna após o uso
display(ranked_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execução em streaming

# COMMAND ----------

# A execução vai apresentar erro ao tentar utilizar rank com streaming. Ela não é suportada.
ranked_df = (spark.readStream
                   .table("bronze")
                   .filter("topic = 'customers'")
                   .select(F.from_json(F.col("value").cast("string"), schema).alias("v"))
                   .select("v.*")
                   .filter(F.col("row_status").isin(["insert", "update"]))
                   .withColumn("rank", F.rank().over(window))
                   .filter("rank == 1")
                   .drop("rank")
             )

(ranked_df.writeStream
            .option("checkpointLocation", f"{bookstore.checkpoint_path}/ranked")
            .trigger(availableNow=True)
            .format("console")
            .start()
)

# COMMAND ----------

# MAGIC %md
# MAGIC Para resulver o problema temos que utilizar batch personalizada.

# COMMAND ----------

from pyspark.sql.window import Window

def batch_upsert(microBatchDF, batchId):
    window = Window.partitionBy("customer_id").orderBy(F.col("row_time").desc())
    
    (microBatchDF.filter(F.col("row_status").isin(["insert", "update"]))
                 .withColumn("rank", F.rank().over(window))
                 .filter("rank == 1")
                 .drop("rank")
                 .createOrReplaceTempView("ranked_updates"))
    
    query = """
        MERGE INTO customers_silver c
        USING ranked_updates r
        ON c.customer_id=r.customer_id
            WHEN MATCHED AND c.row_time < r.row_time
              THEN UPDATE SET *
            WHEN NOT MATCHED
              THEN INSERT *
    """
    
    microBatchDF.sparkSession.sql(query)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS customers_silver
# MAGIC (customer_id STRING, email STRING, first_name STRING, last_name STRING, gender STRING, street STRING, city STRING, country STRING, row_time TIMESTAMP)

# COMMAND ----------

df_country_lookup = spark.read.json(f"{bookstore.dataset_path}/country_lookup")
display(df_country_lookup)

# COMMAND ----------

query = (spark.readStream
                  .table("bronze")
                  .filter("topic = 'customers'")
                  .select(F.from_json(F.col("value").cast("string"), schema).alias("v"))
                  .select("v.*")
                  .join(F.broadcast(df_country_lookup), F.col("country_code") == F.col("code") , "inner") #Para juntar as tabelas de forma otimizada recomenda-se o uso de broadcast (Sempre defina o menor df na função)
               .writeStream
                  .foreachBatch(batch_upsert)
                  .option("checkpointLocation", f"{bookstore.checkpoint_path}/customers_silver")
                  .trigger(availableNow=True)
                  .start()
          )

query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC A utilização do F.broadcast(df_country_lookup) é uma técnica de otimização de performance no Spark conhecida como Broadcast Hash Join. Em um ambiente de processamento distribuído, como o Databricks, um join comum costuma exigir o "shuffle", que é a movimentação de grandes volumes de dados entre os nós do cluster através da rede, um processo caro em termos de tempo e recursos.
# MAGIC
# MAGIC Ao envolver o DataFrame df_country_lookup na função broadcast, você está instruindo o Spark a enviar uma cópia completa dessa tabela para todos os nós (executors) do cluster de uma só vez. Isso é recomendado quando você tem uma tabela pequena (tabela de referência ou lookup, como códigos de países) sendo cruzada com uma tabela muito grande (como a sua tabela "bronze" de clientes). Dessa forma, cada nó processa os dados da tabela grande que já possui localmente com a cópia da tabela pequena, eliminando a necessidade de trocar dados entre servidores durante a execução do join.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verificações

# COMMAND ----------

count = spark.table("customers_silver").count()
expected_count = spark.table("customers_silver").select("customer_id").distinct().count()

assert count == expected_count, "Unit test failed"
print("Unit test passed")
