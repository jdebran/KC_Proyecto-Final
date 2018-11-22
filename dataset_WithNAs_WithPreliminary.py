from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window
import sys

# Recogemos parametros
BUCKET = sys.argv[1]  # 'gs://financials-data-bucket/'
FINANCIALS_FILE = sys.argv[2]  # 'data/2_staging/financials/letters2/initial-csv/'
PRICE_FILE = sys.argv[3]  # 'data/2_staging/price/initial/'
OUTPUT_FILE = sys.argv[4]  # 'data/2_staging/metrics/consolidated_withNAs_withPreliminary/'

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
F.col('COMMON_SIZE_RATIOS__ROE').alias('CSR__ROE'),
F.col('VALUATION_RATIOS__EV_TO_EBIT').alias('VR__EV_TO_EBIT'),
F.col('COMMON_SIZE_RATIOS__ROC_JOEL_GREENBLATT').alias('CSR__ROC_JOEL_GREENBLATT'),
F.col('COMMON_SIZE_RATIOS__OPERATING_MARGIN').alias('CSR__OPERATING_MARGIN'),
F.col('VALUATION_AND_QUALITY__PIOTROSKI_F_SCORE').alias('VQ__PIOTROSKI_F_SCORE'),
F.col('VALUATION_RATIOS__PS_RATIO').alias('VR__PS_RATIO'),
F.col('VALUATION_RATIOS__PB_RATIO').alias('VR__PB_RATIO'),
F.col('VALUATION_AND_QUALITY__ALTMAN_Z_SCORE').alias('VQ__ALTMAN_Z_SCORE'),
F.col('COMMON_SIZE_RATIOS__ROA').alias('CSR__ROA'),
F.col('PER_SHARE_DATA_ARRAY__EARNINGS_PER_SHARE_DILUTED').alias('PSDA__EARNINGS_PER_SHARE_DILUTED'),
F.col('COMMON_SIZE_RATIOS__ROIC').alias('CSR__ROIC'),
F.col('COMMON_SIZE_RATIOS__GROSS_MARGIN').alias('CSR__GROSS_MARGIN'),
F.col('VALUATION_AND_QUALITY__YOY_EPS_GROWTH').alias('VQ__YOY_EPS_GROWTH'),
F.col('VALUATION_AND_QUALITY__YOY_EBITDA_GROWTH').alias('VQ__YOY_EBITDA_GROWTH'),
F.col('PER_SHARE_DATA_ARRAY__EBITDA_PER_SHARE').alias('PSDA__EBITDA_PER_SHARE'),
F.col('PER_SHARE_DATA_ARRAY__TOTAL_DEBT_PER_SHARE').alias('PSDA__TOTAL_DEBT_PER_SHARE'),
F.col('COMMON_SIZE_RATIOS__NET_MARGIN').alias('CSR__NET_MARGIN'),
F.col('INCOME_STATEMENT__REVENUE').alias('IS__REVENUE'),
F.col('INCOME_STATEMENT__NET_INCOME').alias('IS__NET_INCOME'),
F.col('INCOME_STATEMENT__COST_OF_GOODS_SOLD').alias('IS__COST_OF_GOODS_SOLD'),
F.col('BALANCE_SHEET__TOTAL_EQUITY').alias('BS__TOTAL_EQUITY'),
F.col('INCOME_STATEMENT__EBITDA').alias('IS__EBITDA'),
F.col('BALANCE_SHEET__TOTAL_ASSETS').alias('BS__TOTAL_ASSETS'),
F.col('BALANCE_SHEET__LONG_TERM_DEBT').alias('BS__LONG_TERM_DEBT'),
F.col('BALANCE_SHEET__TOTAL_CURRENT_LIABILITIES').alias('BS__TOTAL_CURRENT_LIABILITIES')
)


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


# RANKINGs
auxColumns_cat = [
'PSDA__EARNINGS_PER_SHARE_DILUTED',
'PSDA__EBITDA_PER_SHARE',
'PSDA__TOTAL_DEBT_PER_SHARE',
'CSR__NET_MARGIN',
'IS__REVENUE',
'IS__NET_INCOME', 
'IS__COST_OF_GOODS_SOLD',
'BS__TOTAL_EQUITY',
'IS__EBITDA',
'BS__TOTAL_ASSETS',
'BS__LONG_TERM_DEBT',
'BS__TOTAL_CURRENT_LIABILITIES']

wY = Window.partitionBy('YEAR')
for col in auxColumns_cat:
    TotalDF = TotalDF.withColumn(col + '_RANKING', F.row_number().over(wY.orderBy(F.col(col))))

	

# Agregamos label (rentabilidad periodo siguiente)
TotalDF = TotalDF.withColumn("TARGET", F.lead(F.col("RETURNS"),1).over(Window.partitionBy(F.col("CODE")).orderBy(F.col("YEAR"))))


# Escribimos fichero final (CSV)
TotalDF.repartition('YEAR', 'CODE').write.csv(BUCKET + OUTPUT_FILE, header=True)
