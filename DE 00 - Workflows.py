# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC
# MAGIC
# MAGIC # Workflows
# MAGIC
# MAGIC Let's learn how to use workflows.
# MAGIC
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Learning Objectives:<br>
# MAGIC
# MAGIC By the end of this lesson, you should be able to;
# MAGIC
# MAGIC * Ingest a dataset using raw files from Unity Catalog volumes
# MAGIC * Create a Delta table based on existing data
# MAGIC * Read and update data in a Delta table
# MAGIC * Create a workflow
# MAGIC * Parametrize your workflow
# MAGIC
# MAGIC
# MAGIC ### Dataset
# MAGIC
# MAGIC In this notebook, we will be using the  San Francisco Airbnb rental dataset from <a href="http://insideairbnb.com/get-the-data.html" target="_blank">Inside Airbnb</a>.

# COMMAND ----------

dbutils.widgets.text("neighbourhood", "Mission")

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC
# MAGIC
# MAGIC ## Create and Query  a Delta Table
# MAGIC First we need to read the Airbnb dataset as a Spark DataFrame

# COMMAND ----------

datasets = 'dbfs:/mnt/dbacademy-datasets/scalable-machine-learning-with-apache-spark/v02'
file_path = f"{datasets}/airbnb/sf-listings/sf-listings-2019-03-06-clean.parquet/"
airbnb_df = spark.read.format("parquet").load(file_path)

display(airbnb_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Filter Reviews by Neighbourhood
# MAGIC
# MAGIC Now, we want to filter these reviews per neighbourhood. 
# MAGIC Select a neighboorhood in the column "neighbourhood_cleansed" that you would like to filter and put it in the "selected_neighbourhood" value.

# COMMAND ----------

selected_neighbourhood = dbutils.widgets.get("neighbourhood")

filtered_df = airbnb_df.filter(airbnb_df["neighbourhood_cleansed"] == selected_neighbourhood)
display(filtered_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Let's perform a simple average of the rental prices in your selected neighbourhood.

# COMMAND ----------

from pyspark.sql.functions import lit, avg

username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
avg_price_df = filtered_df.agg(avg("price").alias('avg_price')) \
        .withColumn("neighbourhood", lit(selected_neighbourhood)) \
        .withColumn("user", lit(username))
        
display(avg_price_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write the result to a Delta Table

# COMMAND ----------

shared_catalog = "shared"
shared_schema = "shared"

# with UC
#spark.sql(f"USE CATALOG {shared_catalog}")
#spark.sql(f"USE SCHEMA {shared_schema}")

# With hive
spark.sql(f"USE CATALOG hive_metastore")
spark.sql(f"CREATE DATABASE IF NOT EXISTS {shared_schema}")
spark.sql(f"USE {shared_schema}")

avg_price_df.write.format("delta").mode("append").saveAsTable("avg_rentals_price")
