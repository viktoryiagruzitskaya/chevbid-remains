# Databricks notebook source
#TriggerInfo = getArgument('TriggerInfo')

TriggerInfo = '2020/04/17/aab2850f99de44a696b001b6d9444749'

# COMMAND ----------

from pyspark.sql.types import *
import pandas as pd
import xlrd
import datetime as dt
import numpy as np
import xlsxwriter

PandasHeaderDFCombined = ''

PathApFullSnapshot = "dbfs:/mnt/Refined/MarketIntelligence/CurrencyMappingTable/Archive/{}".format(TriggerInfo)
PathApRefined = "/dbfs/mnt/Refined/MarketIntelligence/CurrencyMappingTable/Full"
PathApRaw = "dbfs:/mnt/Raw/MarketIntelligence/OroniteForecasting/CurrencyExcel/Incremental"

IncrementalFileExists = False
  
#Check if raw incremental folder exists in datalake
try:
  print(dbutils.fs.ls(PathApRaw))
  IncrementalFileExists = True
except:
  IncrementalFileExists = False
  

# COMMAND ----------

def monthToNum(MonthName):
  m = {
        'January' : '01',
        'February' : '02',
        'March' : '03',
        'April' : '04',
        'May' : '05',
        'June' : '06',
        'July' : '07',
        'August' : '08',
        'September' : '09', 
        'October' : '10',
        'November' : '11',
        'December' : '12'
        }
  try:
        monthNum = m[MonthName]
        return monthNum
  except:
        return MonthName

# COMMAND ----------

if IncrementalFileExists:
  
  MonthList = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']

  for Item in dbutils.fs.ls(PathApRaw):
    PandasHeaderDFAllMonth = ''
    for tab_name in MonthList:
      SourceDataDF = pd.read_excel("/dbfs/mnt/Raw/MarketIntelligence/OroniteForecasting/CurrencyExcel/Incremental/{}".format(Item.name), sheet_name=tab_name,skiprows = 5, usecols = "A, B, E", names = ['Target Currency Country','Target Currency','Rate'])
      
      SourceDataDF['Date'] = Item.name[:4] + "-" + monthToNum(tab_name)
 
      if SourceDataDF['Target Currency'].isnull().values.any():
        SourceDataDF = SourceDataDF.loc[: SourceDataDF[(SourceDataDF['Target Currency'] != SourceDataDF['Target Currency'])].index[0] -1 ,:]
    
      CurrencyRatesDF = SourceDataDF[(~SourceDataDF['Target Currency Country'].str.contains('BANK RATE'))].loc[:,['Date','Target Currency','Rate']]
  
      if str(type(PandasHeaderDFAllMonth)) == "<class 'str'>":
        PandasHeaderDFAllMonth = CurrencyRatesDF
      else:
        PandasHeaderDFAllMonth = pd.concat([PandasHeaderDFAllMonth, CurrencyRatesDF ], axis=0, ignore_index=True) 
      
    if str(type(PandasHeaderDFCombined)) == "<class 'str'>":
      PandasHeaderDFCombined = PandasHeaderDFAllMonth
    else:
      PandasHeaderDFCombined = pd.concat([PandasHeaderDFCombined,PandasHeaderDFAllMonth], axis=0, ignore_index=True) 
PandasHeaderDFCombined = PandasHeaderDFCombined.astype({'Rate': 'float'})

# COMMAND ----------

CurrencyMappingSchema = StructType([
                            StructField("Date", StringType(), True)
                            ,StructField("TargetCurrency", StringType(), True)
                           ,StructField("Rate", FloatType(), True)
                         ])

PandasHeaderDFCombined = PandasHeaderDFCombined[pd.to_numeric(PandasHeaderDFCombined['Rate'], errors='coerce').notnull()]
PandasHeaderDFCombined['Rate'] = PandasHeaderDFCombined['Rate'].astype('float')

SparkCurrencyMappingDF  = spark.createDataFrame(PandasHeaderDFCombined, schema=CurrencyMappingSchema)
SparkCurrencyMappingDF.registerTempTable("CurrencyMapping")

# COMMAND ----------

spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

try:
  DFCurrencyMapping = sqlContext.sql("SELECT * FROM CurrencyMapping")
except:
  print("Exception occurred 1166")
  
if DFCurrencyMapping.count() == 0:
  print("No data rows")
else:  
  DFCurrencyMapping.write.format("orc").mode("overwrite").save("dbfs:/mnt/Refined/MarketIntelligence/CurrencyMapping/Full/")
