from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window
import sys

# Recogemos parametros
BUCKET = sys.argv[1]  # 'gs://financials-data-bucket/'
FINANCIALS_FILE = sys.argv[2]  # 'data/2_staging/financials/letters2/initial-csv/'
PRICE_FILE = sys.argv[3]  # 'data/2_staging/price/initial/'
OUTPUT_FILE = sys.argv[4]  # 'data/3_trusted/metrics/initial/'

# Inicializamos contexto
spark = SparkSession.builder.appName('Metrics').getOrCreate()
print('Contexto creado')


### FINANCIALS ###

# Leemos fichero financials (CSV)
financialsDF = spark.read.csv(BUCKET + FINANCIALS_FILE, header=True, inferSchema=True)

# Agregamos YEAR
financialsDF = financialsDF.withColumn('YEAR', F.substring('FISCAL_YEAR', 1, 4)).drop('FISCAL_YEAR')

# Estructura tabla final
# Eliminamos valores negativos
TotalDF = financialsDF.select(
    F.col('CODE'),
    F.col('YEAR'),
    F.col('COMMON_SIZE_RATIOS__ROC_JOEL_GREENBLATT').alias('ROC'),
    F.col('VALUATION_RATIOS__EV_TO_EBIT').alias('EV_TO_EBIT'),
    F.col('VALUATION_AND_QUALITY__MARKET_CAP').alias('CAP'),
    F.col('INCOME_STATEMENT__EPS_BASIC').alias('EPS'),
    F.col('INCOME_STATEMENT__REVENUE').alias('REVENUE'),
    F.col('VALUATION_RATIOS__PE_RATIO').alias('PER'),
    F.col('VALUATION_AND_QUALITY__PIOTROSKI_F_SCORE').alias('PIOTROSKI')
).where('ROC > 0 AND EV_TO_EBIT > 0')



### PRICE ###

# Leemos el fichero price (CSV)
priceDF = spark.read.csv(BUCKET + PRICE_FILE, header=True, inferSchema=True)

# Agregamos YEAR
priceDF = priceDF.withColumn('YEAR', F.year(F.to_date('DATE', 'MM-dd-yyyy')))

# Volatilidad
volatilidadDF = priceDF.groupBy('YEAR', 'CODE').agg(F.stddev('PRICE').alias('RISK'))


# Precio Inicial y Final por empresa y anno
# DataFrame Precio Inicial
precioIniDF = priceDF.sort(F.desc('DATE')).groupBy('YEAR', 'CODE').agg(F.last('PRICE').alias('PRICE_START'))
# DataFrame Precio Final
precioFinDF = priceDF.sort(F.asc('DATE')).groupBy('YEAR', 'CODE').agg(F.last('PRICE').alias('PRICE_END'))
# Join de ambos precios para unirlos a la tabla final
preciosIniFinDF = precioIniDF.join(precioFinDF, on=['YEAR', 'CODE'])

# Rentabilidad
rentabilidadDF = preciosIniFinDF.withColumn('RETURNS', ((F.col('PRICE_END') - F.col('PRICE_START')) / (F.col('PRICE_START'))))

# Join de Rentabilidad y Volatilidad
rentaVolatilDF = rentabilidadDF.join(volatilidadDF, on=['YEAR', 'CODE'])


# Tabla Final
TotalDF = TotalDF.join(rentaVolatilDF, on=['YEAR', 'CODE'])

wY = Window.partitionBy('YEAR')
# ROC_RANKING
TotalDF = TotalDF.withColumn('ROC_RANKING', F.row_number().over(wY.orderBy(F.col('ROC').desc())))

# EBIT_RANKING
TotalDF = TotalDF.withColumn('EBIT_RANKING', F.row_number().over(wY.orderBy(F.col('EV_TO_EBIT'))))


# Escribimos fichero final (CSV)
TotalDF.repartition('YEAR', 'CODE').write.csv(BUCKET + OUTPUT_FILE, header=True)
