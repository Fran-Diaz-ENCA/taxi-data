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

# MARKDOWN ********************

# #### Generación de tabla para la obtención de los ficheros necesarios desde 2009 hasta 2024-03

# CELL ********************

spark.conf.set("spark.sql.parquet.vorder.enabled", "true")
spark.conf.set("spark.microsoft.delta.optimizeWrite.enabled", "true")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# from pyspark.sql import SparkSession
# from pyspark.sql import Row
# from datetime import datetime, timedelta

# # Listas iniciales
# dataset_names = [
#     "Yellow Taxi Trip Records",
#     "Green Taxi Trip Records",
#     "For-Hire Vehicle Trip Records",
#     "High Volume For-Hire Vehicle Trip Records"
# ]

# file_names = [
#     "yellow_tripdata",
#     "green_tripdata",
#     "fhv_tripdata",
#     "fhvhv_tripdata"
# ]

# # Generar todas las combinaciones de años y meses desde 2009 hasta 2024-03
# start_date = datetime(2009, 1, 1)
# end_date = datetime(2024, 3, 1)
# dates = []

# current_date = start_date
# while current_date <= end_date:
#     year_month_str = current_date.strftime("%Y%m")
#     year_month_parquet = current_date.strftime("%Y-%m")
#     dates.append((year_month_str, year_month_parquet))
#     current_date += timedelta(days=32)
#     current_date = current_date.replace(day=1)

# # Crear filas para el DataFrame
# rows = []
# for year_month_str, year_month_parquet in dates:
#     for dataset_name, file_name in zip(dataset_names, file_names):
#         rows.append(Row(year_month=year_month_str, dataset_name=dataset_name, file_name=f"{file_name}_{year_month_parquet}.parquet"))

# # Crear DataFrame
# df = spark.createDataFrame(rows)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# # Definir los datos
# years = list(range(2009, 2025))
# months = [f"{i:02d}" for i in range(1, 13)]
# dataset_names = [
#     "Yellow Taxi Trip Records",
#     "Green Taxi Trip Records",
#     "For-Hire Vehicle Trip Records",
#     "High Volume For-Hire Vehicle Trip Records"
# ]
# file_folders = [
#     "yellow_tripdata",
#     "green_tripdata",
#     "fhv_tripdata",
#     "fhvhv_tripdata"
# ]

# # Generar filas
# data = []
# for year in years:
#     for month in months:
#         if year == 2024 and month == "04":
#             break
#         year_month = f"{year}{month}"
#         for dataset_name, file_folder in zip(dataset_names, file_folders):
#             file_name = f"{file_folder}_{year}-{month}.parquet"
#             data.append((year_month, dataset_name, file_name, file_folder, 1))

# # Crear un DataFrame
# df = spark.createDataFrame(data, ["year_month", "dataset_name", "file_name", "file_folder", "enableToLoad"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import lit

# Definir los datasets y sus fechas de inicio
datasets = [
    ("Yellow Taxi Trip Records", "yellow_tripdata", "yellow_tripdata_", 2009, 1),
    ("Green Taxi Trip Records", "green_tripdata", "green_tripdata_", 2103, 8),
    ("For-Hire Vehicle Trip Records", "fhv_tripdata", "fhv_tripdata_", 2015, 1),
    ("High Volume For-Hire Vehicle Trip Records", "fhvhv_tripdata", "fhvhv_tripdata_", 2019, 2)
]

# Generar las combinaciones de datasets con las fechas correspondientes
data = []
for dataset_name, folder, file_prefix, start_year, start_month in datasets:
    year, month = start_year, start_month
    while (year < 2024) or (year == 2024 and month <= 3):
        date_str = f"{year:04d}-{month:02d}"
        file_name = f"{file_prefix}{date_str}.parquet"
        data.append((dataset_name, file_name, folder, date_str, 1))
        if month == 12:
            year += 1
            month = 1
        else:
            month += 1

# Convertir la lista en un DataFrame de PySpark
columns = ["dataset_name", "file_name", "file_folder", "date", "enabledToLoad"]
df = spark.createDataFrame(data, columns)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import lit, col

# df = df.withColumn("enableToLoad", lit(1))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# df = df.filter(col("year_month") > 202401)#.filter(col("file_name").like("yellow_tripdata%"))
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

system_config_tables = "system_config_tables"
df.write.mode("overwrite").format("delta").saveAsTable(system_config_tables)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC select * from system_config_tables

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
