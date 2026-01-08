# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Professional/main/Includes/images/bronze.png" width="60%">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Criando Ambiente do curso

# COMMAND ----------

# MAGIC %run ../00.Includes/Copy-Datasets

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verificação dos arquivos

# COMMAND ----------

files = dbutils.fs.ls(f"{bookstore.dataset_path}/kafka-raw")
display(files)

# COMMAND ----------

df_raw = spark.read.json(f"{bookstore.dataset_path}/kafka-raw")
display(df_raw)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Injestão de dados na bronze

# COMMAND ----------

# MAGIC %md
# MAGIC ### Criação da função que lê os arquviso e salve localmente no Catalog

# COMMAND ----------

from pyspark.sql import functions as F

def process_bronze():
  
    schema = "key BINARY, value BINARY, topic STRING, partition LONG, offset LONG, timestamp LONG"

    query = (spark.readStream #leitura
                        .format("cloudFiles") #Utiliza Autoloader
                        .option("cloudFiles.format", "json") #Configura tipo de arquivo
                        .schema(schema) #Delimita schema específico
                        .load(f"{bookstore.dataset_path}/kafka-raw")
                        .withColumn("timestamp", (F.col("timestamp")/1000).cast("timestamp")) 
                        .withColumn("year_month", F.date_format("timestamp", "yyyy-MM")) #Extrai ano e mês do timestamp
                        
                  .writeStream #Slavando arquivo
                      .option("checkpointLocation", f"{bookstore.checkpoint_path}/bronze") #local onde será salvo
                      .option("mergeSchema", True) #permite alteração de schema
                      .partitionBy("topic", "year_month") #Particionamento por ano/mes
                      .trigger(availableNow=True)
                      .table("bronze"))
    
    query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Acionando a função

# COMMAND ----------

process_bronze()

# COMMAND ----------

batch_df = spark.table("bronze")
display(batch_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verificando "tabelas diferentes" dentro do mesmo arquivo

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT(topic)
# MAGIC FROM bronze

# COMMAND ----------

# MAGIC %md
# MAGIC ## Adicio Novos Registros

# COMMAND ----------

bookstore.load_new_data()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reprocessamento dos dados

# COMMAND ----------

process_bronze()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verificando contagem de linhas

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM bronze
