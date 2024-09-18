# Databricks notebook source
# DBTITLE 1,Importing Modules
from delta.tables import DeltaTable

# COMMAND ----------

# DBTITLE 1,Created text widgets
dbutils.widgets.text("mode", "")
dbutils.widgets.text("primary_key", "")
dbutils.widgets.text("table_name", "")
dbutils.widgets.text("database" , "")
dbutils.widgets.text("source_table", "")

# COMMAND ----------

# DBTITLE 1,Getting the widgets
mode = dbutils.widgets.get("mode")
primary_key = dbutils.widgets.get("primary_key")
table_name = dbutils.widgets.get("table_name")
database = dbutils.widgets.get("database")
source_table = dbutils.widgets.get("source_table")

# COMMAND ----------

# DBTITLE 1,Secrets
spark.conf.set("user",  "pratibha12345" )
spark.conf.set("password", "Pratibha1234")

# COMMAND ----------

# DBTITLE 1,SQL connection
database_host = "pratibhaserver.database.windows.net"
database_port = "1433"
database_name = "pratibhadatabase"
table = source_table
user = spark.conf.get("user")
password = spark.conf.get("password")
url = f"jdbc:sqlserver://{database_host}:{database_port};databaseName={database_name}"

# COMMAND ----------

# DBTITLE 1,Reading the table from database
try:
    student_df = (spark.read
                    .format("jdbc")
                    .option("url", url)
                    .option("dbtable", table)
                    .option("user", user)
                    .option("password", password)
                    .load())
    display(student_df)

except Exception as e:
    print(f"An error occurred while connecting to the database or loading data: {str(e)}")

# COMMAND ----------

# DBTITLE 1,Function with upsert logic
def write_table(df, mode, table_name, database, primary_key=None):
    full_table_name = f"{database}.{table_name}"
    if mode == "overwrite":
        df.write.format("delta").mode(mode).saveAsTable(full_table_name)
        print("Overwrite completed")
    elif mode == "append":
        df.write.format("delta").mode(mode).saveAsTable(full_table_name)
        print("Append completed")
    elif mode == "upsert":
        if primary_key is None:
            raise ValueError("primary_key must be provided for upsert mode.")

        else:
            if isinstance(primary_key, str):
                primary_key = [primary_key]
            deltaTable = DeltaTable.forName(spark, full_table_name)
            matchKeys = " AND ".join(f"old.{col} = new.{col}" for col in primary_key)
            deltaTable.alias("old") \
                .merge(df.alias("new"),matchKeys) \
                .whenMatchedUpdateAll().whenNotMatchedInsertAll() \
                .execute()
            print(f"Data upserted into Delta table at {full_table_name}.")
    else:
        raise ValueError("Invalid mode. Use 'overwrite', 'append', or 'upsert'.")

# COMMAND ----------

# DBTITLE 1,Calling the Function
write_table(remote_table, mode, table_name, database, primary_key)

# COMMAND ----------

# DBTITLE 1,Final Result
# MAGIC %sql
# MAGIC SELECT * FROM target_fhpl.student