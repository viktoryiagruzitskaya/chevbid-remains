# Databricks notebook source
MnemonicString = getArgument('MnemonicString')
#MnemonicString = 'WPU061;WPU03;PCU327410327410;WPU06;WPU03THRU15'

# COMMAND ----------

from pyspark.sql.types import *
import pandas as pd
import xlrd
import datetime as dt
import numpy as np
import xlsxwriter
import re
import requests
from bs4 import BeautifulSoup as bs

# COMMAND ----------

def monthToNum(shortMonth):
  m = {
        'Jan' : '01',
        'Feb' : '02',
        'Mar' : '03',
        'Apr' : '04',
        'May' : '05',
        'Jun' : '06',
        'Jul' : '07',
        'Aug' : '08',
        'Sep' : '09', 
        'Oct' : '10',
        'Nov' : '11',
        'Dec' : '12'
        }
  try:
        monthNum = m[shortMonth]
        return monthNum
  except:
        return shortMonth

# COMMAND ----------

def processBLSPage(pageUrl):
  
    r = requests.get(pageUrl)
    soup = bs(r.content, 'html.parser')
    
    #get headers
    colNames = soup.findAll("th", {"id": re.compile("^col\d*$")}  ) 
    headings = []
    for td in colNames:
      if td.text:
        headings.append(monthToNum(td.text.replace('\n', ' ').strip()))
      
    #get table with prices
    BLSResponseSet = soup.findAll("tr",attrs={"class": {"even","odd"}}) 
    PandasHeaderList  = []
    
    for CurrentRow in BLSResponseSet:
      YearValue = CurrentRow.find("th").text # read year
      t_row = {}
      t_row['Year'] = YearValue
      for val, header in zip(CurrentRow.find_all("td"),headings):# read year
        #print('header: {} -> value {}'.format(header,val))
        t_row[header] = val.text.replace('\n', '').strip()
      PandasHeaderList.append(t_row)
      
    PandasHeaderDF = pd.DataFrame(PandasHeaderList)
    return PandasHeaderDF
#processBLSPage('https://data.bls.gov/timeseries/WPU06?amp%253bdata_tool=XGtable&output_view=data&include_graphs=false')

# COMMAND ----------

PandasHeaderDFCombined = ''
for Mnemonic in MnemonicString.split(';'):
  #print('https://data.bls.gov/timeseries/{}?amp%253bdata_tool=XGtable&output_view=data&include_graphs=false'.format(Mnemonic))
  pageUrl = 'https://data.bls.gov/timeseries/{}?amp%253bdata_tool=XGtable&output_view=data&include_graphs=false'.format(Mnemonic)
  try:
    SourcePandasDF = processBLSPage(pageUrl)
  except KeyError: 
     print("An error accured while getting response from BLS system. /n Series Id: {} /n Page URL: {} in Currency Mapping table.".format(Mnemonic, pageUrl) )
      
  PandasDFPivoted = pd.melt(SourcePandasDF , id_vars = ['Year'],var_name = 'Month', value_name='Price')
  PandasDFPivoted['Date'] = PandasDFPivoted['Year'].apply(str) + "-" + PandasDFPivoted['Month']
  PandasDFPivoted['Mnemonic'] = Mnemonic
  PandasDFPivoted['ForecastingFlag'] = "N"
  PandasDFPivoted.loc[PandasDFPivoted['Price'].str.contains("\(P\)"), 'ForecastingFlag'] = "Y"
  PandasDFPivoted['Price'] = PandasDFPivoted['Price'].str.replace("\(P\)","")
  PandasDFPivoted['Price'] = PandasDFPivoted['Price'].str.replace("\(C\)","")
  PandasDFPivoted['Source'] = "BLS"
  SupplierSource = PandasDFPivoted[['Mnemonic','Date','Price','ForecastingFlag','Source']]
  
  if str(type(PandasHeaderDFCombined)) == "<class 'str'>":
      PandasHeaderDFCombined = SupplierSource.head(1)
      SupplierSource = SupplierSource[1:]

  PandasHeaderDFCombined = pd.concat([PandasHeaderDFCombined, SupplierSource], axis=0)

# COMMAND ----------

SourceSchema = StructType([
                            StructField("Mnemonic", StringType(), True)
                           ,StructField("Date", StringType(), True)
                           ,StructField("Price", StringType(), True)
                           ,StructField("ForecastFlag", StringType(), True)
                           ,StructField("Source", StringType(), True)
                         ])
SparkSourceBLSDF  = spark.createDataFrame(PandasHeaderDFCombined, schema=SourceSchema)

SparkSourceBLSDF.registerTempTable("SourceDataBLS")

# COMMAND ----------

spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

try:
  DFSourceBLS = sqlContext.sql("SELECT Mnemonic, Date, CAST(Price AS decimal(38,18)) AS Price, ForecastFlag, Source,Source AS Part FROM SourceDataBLS")
except:
  print("Exception occurred 1166")
  
if DFSourceBLS.count() == 0:
  print("No data rows")
else:  
  DFSourceBLS.write.partitionBy("Part").format("orc").mode("overwrite").save("dbfs:/mnt/Refined/MarketIntelligence/IndexMarketPrice/Full/")
