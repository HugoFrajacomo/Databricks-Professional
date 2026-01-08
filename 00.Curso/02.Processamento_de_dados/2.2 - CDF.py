# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Professional/main/Includes/images/CDF.png" width="60%">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delta lake Changes Data Feed (CDF)
# MAGIC
# MAGIC Definição: CDF gera automaticamente CDC em tabelas delta lake
# MAGIC
# MAGIC Salva todas as mudanças de linhas para todo dado escritos em uma Delta tabel
# MAGIC
# MAGIC OBS: utilize apenas quando as tabelas forem atualizadas com updates, delete ou include. Não utilize em tabelas que as modanças são appends apenas

# COMMAND ----------

# MAGIC %run ../00.Includes/Copy-Datasets

# COMMAND ----------

# MAGIC %md
# MAGIC ## Abilitando CDF

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE customers_silver 
# MAGIC SET TBLPROPERTIES (delta.enableChangeDataFeed = true); -- utilize essa propriedade para ativar o CDC

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE EXTENDED customers_silver

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY customers_silver

# COMMAND ----------

# MAGIC %md
# MAGIC IMPORTANTE: CASO UTILIZE O COMANDO VACUUM, PERDERÁ OS REGISTROS DAS MODIFICAÇÕES

# COMMAND ----------

bookstore.load_new_data()
bookstore.process_bronze()
bookstore.process_orders_silver()
bookstore.process_customers_silver()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM table_changes("customers_silver", 2) --Define a partir de qual versão queremos ver as alterações

# COMMAND ----------

# MAGIC %md
# MAGIC Outras opções: 
# MAGIC
# MAGIC - Utilizando versão incial e final:
# MAGIC `
# MAGIC SELECT * FROM table_changes('table_name', start_version,[end_version])
# MAGIC `
# MAGIC - Utilizando versão datas específicas:
# MAGIC `
# MAGIC SELECT * FROM table_changes('table_name', start_timestamp,[end_timestamp])
# MAGIC `
# MAGIC

# COMMAND ----------

bookstore.load_new_data()
bookstore.process_bronze()
bookstore.process_orders_silver()
bookstore.process_customers_silver()

# COMMAND ----------

cdf_df = (spark.readStream
               .format("delta")
               .option("readChangeData", True) # Para visualizar os dados de alteração
               .option("startingVersion", 3) # Traz os dados a partir da versão 2
               .table("customers_silver"))

display(cdf_df, checkpointLocation = f"{bookstore.checkpoint_path}/tmp/cdf_{time.time()}")
