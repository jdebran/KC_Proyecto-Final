import pyspark
import pyspark.sql.functions as F
from pyspark.sql.functions import lit,create_map
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.window import Window
from itertools import chain
import sys

sc = pyspark.SparkContext("local[*]")
sqlContext = pyspark.sql.SQLContext(sc)

print('Contexto creado')

# Recogemos parametros
INPUT_CSV = sys.argv[1]  # 'gs://financials-data-bucket/data/GSPC-2.csv'
OUTPUT_CSV = sys.argv[2] # gs://financials-data-bucket/data/2_staging/index_anuals.csv

indicesDF = sqlContext.read.format('csv') \
  .options(header='true', inferSchema='true') \
  .load(INPUT_CSV)

print(INPUT_CSV + ' fichero de entrada leido')

indicesDF = indicesDF.select( \
F.col('Date').alias('DATE'), \
F.col('Open').alias('OPEN'), \
F.col('High').alias('HIGH'), \
F.col('Low').alias('LOW'),\
F.col('Close').alias('CLOSE'),\
F.col('Adj Close').alias('ADJ_CLOSE'),
F.col('Volume').alias('VOLUME'))
indicesDF = indicesDF.withColumn('YEAR', F.substring('DATE', 1, 4).cast(IntegerType()))
indicesDF = indicesDF.withColumn('INDICE', lit(1))
indicesDF = indicesDF.filter("Date > '1970-01-01'").sort(F.asc('Date'))

print('Columnas creadas')

#Rentabilidad diaria
windowSpec = Window.orderBy(F.col("DATE")).rowsBetween(-1, 0)
indicesDF = indicesDF.withColumn('AUX', F.sum("ADJ_CLOSE").over(windowSpec))
indicesDF = indicesDF.withColumn("RETURNS", (F.col("ADJ_CLOSE") - (F.col("AUX")-F.col("ADJ_CLOSE"))) / (F.col("AUX")-F.col("ADJ_CLOSE"))).drop("AUX")

print('Rentabilidad diaria')

#Rentabilidad acumulada
precioIniDF = indicesDF.sort(F.desc('DATE')).groupBy('YEAR').agg(F.last('OPEN').alias('PRICE_START'))
indicesDF = indicesDF.join(precioIniDF, on=['YEAR'])
indicesDF = indicesDF.withColumn('CUMULATIVE_RETURNS', ((F.col('ADJ_CLOSE') - F.col('PRICE_START')) / (F.col('PRICE_START'))))

print('Rentabilidad acumulada')

# Escribimos fichero final
r_acuDF \
.write.format("com.databricks.spark.csv") \
.option("header", "true") \
.save(OUTPUT_CSV)

print('Fin')


