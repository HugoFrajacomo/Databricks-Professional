# Databricks notebook source
# MAGIC %run ../00.Includes/Copy-Datasets

# COMMAND ----------

# MAGIC %sql 
# MAGIC DESCRIBE TABLE customers_silver

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW customers_vw AS
# MAGIC   SELECT
# MAGIC     customer_id,
# MAGIC     CASE 
# MAGIC       WHEN is_member('admins_demo') THEN email --atribuia a permissão para somente administradores terem acesso a visualização do email
# MAGIC       ELSE 'REDACTED'
# MAGIC     END AS email,
# MAGIC     gender,
# MAGIC     CASE 
# MAGIC       WHEN is_member('admins_demo') THEN first_name
# MAGIC       ELSE 'REDACTED'
# MAGIC     END AS first_name,
# MAGIC     CASE 
# MAGIC       WHEN is_member('admins_demo') THEN last_name
# MAGIC       ELSE 'REDACTED'
# MAGIC     END AS last_name,
# MAGIC     CASE 
# MAGIC       WHEN is_member('admins_demo') THEN street
# MAGIC       ELSE 'REDACTED'
# MAGIC     END AS street,
# MAGIC     city,
# MAGIC     country,
# MAGIC     row_time
# MAGIC   FROM customers_silver

# COMMAND ----------

# MAGIC %md
# MAGIC obs: Os grupos são definidos no console administrativo. O Free edition não da acesso ao recurso.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM customers_vw

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW customers_fr_vw AS
# MAGIC SELECT * FROM customers_vw
# MAGIC WHERE 
# MAGIC   CASE 
# MAGIC     WHEN is_member('admins_demo') THEN TRUE -- Caso for administrator ele ve normalmente todos os paises registrados.
# MAGIC     ELSE country = "France" AND row_time > "2022-01-01" -- Caso contrário todos os dados vão ser filtrados para france com row_time > 2022-01-01
# MAGIC   END

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM customers_fr_vw

# COMMAND ----------

# MAGIC %md
# MAGIC ## Funções para aplicar mascara em colunas

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE FUNCTION custumer_city(city STRING)
# MAGIC RETURN CASE WHEN is_member('admins_demo') THEN city ELSE 'REDACTED' END

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE customers_silver ALTER COLUMN city SET MASK custumer_city

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM customers_silver

# COMMAND ----------

# MAGIC %md
# MAGIC ## Funções para aplicar mascara em linhas

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE FUNCTION ct_filter(country STRING)
# MAGIC RETURN IF(is_member('admins_demo'), true, country='France');

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE customers_silver SET ROW FILTER ct_filter ON (country);

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM customers_silver
