# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## ![test](https://redislabs.com/wp-content/uploads/2016/12/lgo-partners-databricks-125x125.png) Rapid Start Demo 

# COMMAND ----------

# MAGIC %md
# MAGIC this is a demo

# COMMAND ----------

# MAGIC %fs ls /databricks-datasets/Rdatasets/data-001/csv/ggplot2

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Exploration

# COMMAND ----------

# Creating a dataframe
# maintain headers and schema
# Transformation
datapath = "/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv"
diamondsDF = spark.read.format("csv")\
              .option("header","true")\
              .option("inferschema","true")\
              .load(datapath)

# COMMAND ----------

diamondsDF.count()

# COMMAND ----------

# Action (execution happens here)
display(diamondsDF)
#diamondsDF.show()
#download option (not recommended more than 1 mn or 10mn rows)

# COMMAND ----------

# Different functionality within the cell
# Plotting and options
display(diamondsDF)

# COMMAND ----------

diamondsDF.createOrReplaceTempView("diamonds_table")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS diamonds
# MAGIC   USING csv
# MAGIC   OPTIONS (path "/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv", header "true")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from diamonds_table where cut = "Good"
# MAGIC limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct (cut) from diamonds_table

# COMMAND ----------

# MAGIC %md
# MAGIC # Widgets

# COMMAND ----------

dbutils.widgets.dropdown("cut", "Premium", ["Premium", "Ideal", "Good", "Fair", "Very Good"])

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from diamonds_table where cut = getArgument("cut")

# COMMAND ----------

