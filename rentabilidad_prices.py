import pyspark
import pyspark.sql.functions as F
from pyspark.sql.functions import lit,create_map
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, DateType, LongType, StringType
from itertools import chain
import sys


sc = pyspark.SparkContext("local[*]")
sqlContext = pyspark.sql.SQLContext(sc)

# Recogemos parametros
INPUT_CSV = sys.argv[1]  # 'gs://financials-data-bucket/data/2_staging/price/initial/'
OUTPUT_CSV = sys.argv[2]  # 'gs://financials-data-bucket/data/3_trusted/price/initial/'

print('Contexto creado')

priceDF = sqlContext.read.format('csv') \
  .options(header='true', inferSchema='true') \
  .load(INPUT_CSV)

print('Archivo cargado')

priceDF = priceDF.select( \
F.col('code').alias('CODE'), \
F.col('Price').alias('PRICE'), \
F.col('Date').alias('DATE'))
priceDF = priceDF.withColumn('YEAR', F.year(F.to_date('DATE', 'MM-dd-yyyy')))

print('Dataset creado')

#Rentabilidad diaria
windowSpec = Window.partitionBy("CODE").orderBy(F.col('DATE').asc()).rowsBetween(-1,0)
priceDF = priceDF.withColumn('AUX', F.sum("price").over(windowSpec))
priceDF = priceDF.withColumn("RETURNS", (F.col("PRICE") - (F.col("AUX") - F.col("PRICE"))) / (F.col("AUX") - F.col("PRICE"))).drop("AUX")

print('Rentabilidad diaria')

#Rentabilidad acumulada
precioIniDF = priceDF.sort(F.desc('DATE')).groupBy('YEAR', 'CODE').agg(F.last('PRICE').alias('PRICE_START'))
priceDF = precioIniDF.join(priceDF, on=['YEAR', 'CODE'])
priceDF = priceDF.withColumn('CUMULATIVE_RETURNS', ((F.col('PRICE') - F.col('PRICE_START')) / (F.col('PRICE_START'))))

print('Rentabilidad acumulada')

priceDF.repartition('YEAR', 'CODE').write.csv(OUTPUT_CSV, header=True)

print('Fichero guardado')

sc.stop()