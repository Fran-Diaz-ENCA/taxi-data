# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "f0225bed-f40c-4634-bac5-a1b4e4af9594",
# META       "default_lakehouse_name": "lh_taxi_data",
# META       "default_lakehouse_workspace_id": "00c05ec9-97b8-47d0-9d6a-3776e00b613d",
# META       "known_lakehouses": [
# META         {
# META           "id": "f0225bed-f40c-4634-bac5-a1b4e4af9594"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

spark.conf.set("spark.sql.parquet.vorder.enabled", "true")
spark.conf.set("spark.microsoft.delta.optimizeWrite.enabled", "true")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

pathBronze = "Files/bronze/yellow_taxi_tr/"
pathTables = "Tables/"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.parquet(f"{pathBronze}yellow_tripdata_2024-01.parquet")
# display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

yellow_table_path = "silver_yellow_taxt_tr"
df.write.mode("append").format("delta").saveAsTable(yellow_table_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# df = spark.sql("SELECT * FROM tcl_trip_lakehouse.silver_yellow_taxt_tr LIMIT 1000")
# display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select * from silver_yellow_taxt_tr

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
