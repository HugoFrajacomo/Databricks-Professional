# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Professional/main/Includes/images/books_sales.png" width="60%">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC Nesta modalidade de Stream-Static join a tabela em streaming será responsável por acionar o trigger de processamento que juntará com as informações da tabela executada em batch

# COMMAND ----------

# MAGIC %run ../00.Includes/Copy-Datasets

# COMMAND ----------

from pyspark.sql import functions as F

def process_books_sales():
    #Tabela em streaming
    orders_df = (spark.readStream.table("orders_silver")
                        .withColumn("book", F.explode("books"))
                )

    #Tabela em batch
    books_df = spark.read.table("current_books")

    query = (orders_df
                  .join(books_df, orders_df.book.book_id == books_df.book_id, "inner")
                  .writeStream
                     .outputMode("append")
                     .option("checkpointLocation", f"{bookstore.checkpoint_path}/books_sales")
                     .trigger(availableNow=True)
                     .table("books_sales")
    )

    query.awaitTermination()
    
process_books_sales()

# COMMAND ----------

# MAGIC %md
# MAGIC OBS: caso adicione mais uma linha na tabela em batch o dado final da tabela pós join não será atualizado. Esta é uma limitação do Stream-Static Joins. 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM books_sales

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM books_sales

# COMMAND ----------

bookstore.load_new_data()
bookstore.process_bronze()
bookstore.process_books_silver()
bookstore.process_current_books()

process_books_sales()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM books_sales

# COMMAND ----------

bookstore.process_orders_silver()

process_books_sales()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM books_sales
