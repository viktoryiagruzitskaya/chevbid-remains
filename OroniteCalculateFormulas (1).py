# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import explode
from datetime import date, timedelta, datetime
import pandas as pd
import numpy as np
import xlrd

# COMMAND ----------

#######################         Read Formula configuration            ############################
IndexPandasDf = pd.read_excel("/dbfs/mnt/Raw/MarketIntelligence/OroniteForecasting/FormulaConfiguration/Incremental/Formula Configuration.xlsx", sheet_name="Formulas",
                           converters={'Material Nbr': str, 
                                       'Material Description': str, 
                                       'Material Owner': str, 
                                       'Supplier': str, 
                                       'Input1': str, 
                                       'Input1 UOM': str,
                                       'Input1 Avg (Month)':str,
                                       'Input1 Delay (Month)':str,
                                       'Input2': str, 
                                       'Input2 UOM': str,
                                       'Input2 Avg (Month)':str,
                                       'Input2 Delay (Month)':str,
                                       'Input3': str, 
                                       'Input3 UOM': str,
                                       'Input3 Avg (Month)':str,
                                       'Input3 Delay (Month)':str,
                                       'Input4': str,
                                       'Input4 UOM': str,
                                       'Input4 Avg (Month)':str,
                                       'Input4 Delay (Month)':str,
                                       'Input5': str,
                                       'Input5 UOM': str, 
                                       'Input5 Avg (Month)':str,
                                       'Input5 Delay (Month)':str,
                                       'Constant1': float, 
                                       'Constant1 UOM': str, 
                                       'Constant2': float, 
                                       'Constant2 UOM': str, 
                                       'Constant3': float, 
                                       'Constant3 UOM': str, 
                                       'Constant4': float, 
                                       'Constant4 UOM': str, 
                                       'Constant5': float, 
                                       'Constant5 UOM': str, 
                                       'Constant6': float, 
                                       'Constant6 UOM': str, 
                                       'Formula': str, 
                                       'Formula UOM': str,
                                       'Quarterly': str

                                      }
                               ) 

IndexSchema = StructType([
                            StructField("Material Nbr", StringType(), True)
                           ,StructField("Material Description", StringType(), True)
                           ,StructField("Material Owner", StringType(), True)
                           ,StructField("Supplier", StringType(), True)
                           ,StructField("Input1", StringType(), True)
                           ,StructField("Input1 UOM", StringType(), True)
                           ,StructField("Input1 Avg (Month)", StringType(), True)
                           ,StructField("Input1 Delay (Month)", StringType(), True)
                           ,StructField("Input2", StringType(), True)
                           ,StructField("Input2 UOM", StringType(), True)
                           ,StructField("Input2 Avg (Month)", StringType(), True)
                           ,StructField("Input2 Delay (Month)", StringType(), True)
                           ,StructField("Input3", StringType(), True)
                           ,StructField("Input3 UOM", StringType(), True)
                           ,StructField("Input3 Avg (Month)", StringType(), True)
                           ,StructField("Input3 Delay (Month)", StringType(), True)
                           ,StructField("Input4", StringType(), True)
                           ,StructField("Input4 UOM", StringType(), True)
                           ,StructField("Input4 Avg (Month)", StringType(), True)
                           ,StructField("Input4 Delay (Month)", StringType(), True)
                           ,StructField("Input5", StringType(), True)
                           ,StructField("Input5 UOM", StringType(), True)
                           ,StructField("Input5 Avg (Month)", StringType(), True)
                           ,StructField("Input5 Delay (Month)", StringType(), True)                           
                           ,StructField("Constant1", FloatType(), True)
                           ,StructField("Constant1 UOM", StringType(), True)
                           ,StructField("Constant2", FloatType(), True)
                           ,StructField("Constant2 UOM", StringType(), True)
                           ,StructField("Constant3", FloatType(), True)
                           ,StructField("Constant3 UOM", StringType(), True)
                           ,StructField("Constant4", FloatType(), True)
                           ,StructField("Constant4 UOM", StringType(), True)
                           ,StructField("Constant5", FloatType(), True)
                           ,StructField("Constant5 UOM", StringType(), True)
                           ,StructField("Constant6", FloatType(), True)
                           ,StructField("Constant6 UOM", StringType(), True)
                           ,StructField("Formula", StringType(), True)
                           ,StructField("Formula UOM", StringType(), True)
                           ,StructField("Quarterly", StringType(), True)
                         ])


SparkIndexDF  = spark.createDataFrame(IndexPandasDf, schema=IndexSchema)

SparkIndexDF.registerTempTable("IndexFile")

#Read Base Prices for both Mnemonics and Materials
BasePricesPandasDf = pd.read_excel("/dbfs/mnt/Raw/MarketIntelligence/OroniteForecasting/FormulaConfiguration/Incremental/Formula Configuration.xlsx", sheet_name="BasePrices",
                           converters={'Mnemonic/Material Number': str, 
                                       'Supplier': str, 
                                       'EffectiveDate': str, 
                                       'ExpiresDate': str, 
                                       'BasePrice': float, 
                                       'BasePrice UOM': str, 
                                       'Price Q[0]': float
                                      }
                               ) 

BasePricesSchema = StructType([
                            StructField("Mnemonic/Material Number", StringType(), True)
                           ,StructField("Supplier", StringType(), True)
                           ,StructField("EffectiveDate", StringType(), True)
                           ,StructField("ExpiresDate", StringType(), True)
                           ,StructField("BasePrice", FloatType(), True)
                           ,StructField("BasePrice UOM", StringType(), True)
                           ,StructField("Price Q[0]", FloatType(), True)
                         ])
SparkBasePricesDF  = spark.createDataFrame(BasePricesPandasDf, schema=BasePricesSchema)

SparkBasePricesDF.registerTempTable("BasePrices")

#Read Proxy Mnemonics

ProxyMnemonicsPandasDf = pd.read_excel("/dbfs/mnt/Raw/MarketIntelligence/OroniteForecasting/FormulaConfiguration/Incremental/Formula Configuration.xlsx", sheet_name="ProxyIndexes",
                           converters={'Material Nbr': str,                                       
                                       'Material Owner': str, 
                                       'Supplier': str, 
                                       'Mnemonic': str,
									   'ProxyMnemonic1': str, 
                                       'ProxyMnemonic1UOM': str,                                      
                                       'ProxyMnemonic2': str, 
                                       'ProxyMnemonic2UOM': str,
                                       'ProxyMnemonic3': str, 
                                       'ProxyMnemonic3UOM': str,
                                      }
                               ) 

ProxyMnemonicsSchema = StructType([
                            StructField("Material Nbr", StringType(), True)
                           ,StructField("Material Owner", StringType(), True)
                           ,StructField("Supplier", StringType(), True)
                           ,StructField("Mnemonic", StringType(), True)
                           ,StructField("ProxyMnemonic1", StringType(), True)
                           ,StructField("ProxyMnemonic1UOM", StringType(), True)
                           ,StructField("ProxyMnemonic2", StringType(), True)
                           ,StructField("ProxyMnemonic2UOM", StringType(), True)
                           ,StructField("ProxyMnemonic3", StringType(), True)
                           ,StructField("ProxyMnemonic3UOM", StringType(), True)
                         ])
SparkProxyMnemonicsDF  = spark.createDataFrame(ProxyMnemonicsPandasDf, schema=ProxyMnemonicsSchema)
SparkProxyMnemonicsDF.registerTempTable("ProxyMnemonics")

#Read Mnemonic Market Prices
DFSparkSource = spark.read.format("orc").option("header", "true").load("dbfs:/mnt/Refined/MarketIntelligence/IndexMarketPrice/Full/")
DFSparkSource.registerTempTable("SourceData")

#Generate dates for the Forecast data (from last available till today() + 3 days)
ForecastDatesDF = spark.sql("""SELECT Mnemonic,  CASE  TO_DATE(Max(SourceData.Date), 'yyyy-MM') < add_months(CAST(current_timestamp() as DATE),3) 
                                                        WHEN true THEN sequence(
                                                                        add_months(TO_DATE(Max((SourceData.Date)), 'yyyy-MM'),1), 
                                                                        add_months(CAST(current_timestamp() as DATE),3), 
                                                                        interval 1 month) end DateForJoin 
                              FROM SourceData 
                              GROUP BY Mnemonic 
                              HAVING TO_DATE(Max(SourceData.Date), 'yyyy-MM') < add_months(CAST(current_timestamp() as DATE),3)""").withColumn("DateForJoin", explode("DateForJoin"))
ForecastDatesDF.registerTempTable("Dates")

#Read Currency Mapping table
DFCurrencyMapping = spark.read.format("orc").option("header", "true").load("dbfs:/mnt/Refined/MarketIntelligence/CurrencyMapping/Full/")
DFCurrencyMapping.registerTempTable("CurrencyMapping")

PDFCurrencyMapping = DFCurrencyMapping.select("*").toPandas().rename(columns={"TargetCurrency": "Currency"})
CurrencyMappingPDf = PDFCurrencyMapping.pivot('Date','Currency','Rate')

#Read UOM Mapping table
UOMMappingPandasDf = pd.read_excel("/dbfs/mnt/Raw/MarketIntelligence/OroniteForecasting/UOMMappingTable/Incremental/UOM Mapping Table.xlsx", sheet_name="UOM Conversion",
                           converters={'UOM': str, 
                                       'TargetUOM': str, 
                                       'Ratio': float
                                      }
                               ) 

UOMMappingSchema = StructType([
                            StructField("UOM", StringType(), True)
                           ,StructField("TargetUOM", StringType(), True)
                           ,StructField("Ratio", FloatType(), True)
                         ])
#Create UOM conversion dictionary
UOMDict = UOMMappingPandasDf.loc[UOMMappingPandasDf['TargetUOM'] == "MT"].set_index('UOM')['Ratio'].to_dict()

SparkUOMMappingDF  = spark.createDataFrame(UOMMappingPandasDf, schema=UOMMappingSchema)

SparkUOMMappingDF.registerTempTable("UOMMapping")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Mapping table for Quarterly calculations
# MAGIC CREATE OR REPLACE TEMPORARY VIEW MonthToQuarterMapping 
# MAGIC AS
# MAGIC SELECT '01' AS Month, 'Q1' AS Quarter  UNION ALL
# MAGIC SELECT '02' AS Month, 'Q1' AS Quarter  UNION ALL
# MAGIC SELECT '03' AS Month, 'Q1' AS Quarter  UNION ALL
# MAGIC SELECT '04' AS Month, 'Q2' AS Quarter  UNION ALL
# MAGIC SELECT '05' AS Month, 'Q2' AS Quarter  UNION ALL
# MAGIC SELECT '06' AS Month, 'Q2' AS Quarter  UNION ALL
# MAGIC SELECT '07' AS Month, 'Q3' AS Quarter  UNION ALL
# MAGIC SELECT '08' AS Month, 'Q3' AS Quarter  UNION ALL
# MAGIC SELECT '09' AS Month, 'Q3' AS Quarter  UNION ALL
# MAGIC SELECT '10' AS Month, 'Q4' AS Quarter  UNION ALL
# MAGIC SELECT '11' AS Month, 'Q4' AS Quarter  UNION ALL
# MAGIC SELECT '12' AS Month, 'Q4' AS Quarter 

# COMMAND ----------

# MAGIC %sql
# MAGIC --Cleansing 
# MAGIC CREATE OR REPLACE TEMPORARY VIEW BasePricesSource 
# MAGIC AS
# MAGIC SELECT 
# MAGIC      NULLIF(`Mnemonic/Material Number`       , 'NaN')                                  AS MnemonicOrMaterialNumber
# MAGIC     ,NULLIF(Supplier                         , 'NaN')                                  AS Supplier
# MAGIC     ,TO_DATE(NVL(NULLIF(EffectiveDate        , 'NaN'), '1900-01-01'))                  AS EffectiveDate
# MAGIC     ,TO_DATE(NVL(NULLIF(ExpiresDate          , 'NaN'), '2999-01-01'))                  AS ExpiresDate
# MAGIC     ,NULLIF(BasePrice                        , 'NaN')                                  AS BasePrice
# MAGIC     ,NULLIF(`BasePrice UOM`                  , 'NaN')                                  AS BasePriceUOM
# MAGIC     ,NULLIF(`Price Q[0]`                     , 'NaN')                                  AS PriceQ0
# MAGIC     ,NULL                                                                              AS PriceQ0UOM
# MAGIC FROM BasePrices

# COMMAND ----------

# MAGIC %sql                                                            
# MAGIC CREATE OR REPLACE TEMPORARY VIEW ForecastDataset1 AS
# MAGIC -- find last 3 month with non-forecast prices for each Mnemonic (average of them is used as Forecast Price)
# MAGIC WITH CTE AS (
# MAGIC SELECT Mnemonic
# MAGIC ,Date
# MAGIC ,TO_DATE(Date, 'yyyy-MM') AS DateForJoin
# MAGIC ,Price
# MAGIC ,ForecastFlag
# MAGIC ,Source
# MAGIC --if Mnemonic exists in both Manual and any other file, we use NOT Manual source
# MAGIC ,ROW_NUMBER() OVER (PARTITION BY Mnemonic, Date ORDER BY CASE WHEN Source LIKE 'Manual%' THEN 2 ELSE 1 END) RowNumberForFilter
# MAGIC FROM SourceData)
# MAGIC SELECT
# MAGIC Mnemonic
# MAGIC ,Date
# MAGIC ,DateForJoin
# MAGIC ,Price
# MAGIC ,ForecastFlag
# MAGIC ,Source
# MAGIC ,ROW_NUMBER() OVER (PARTITION BY Mnemonic ORDER BY CASE WHEN ForecastFlag = 'N' THEN 1 ELSE 2 END, Date desc) RowNumberForPriceFilter 
# MAGIC FROM CTE
# MAGIC WHERE RowNumberForFilter = 1

# COMMAND ----------

# MAGIC %sql    
# MAGIC -- #######################         Calculate Forecast Prices and get Base Prices for mnemonics            #############################
# MAGIC CREATE OR REPLACE TEMPORARY VIEW TransformedSource
# MAGIC AS
# MAGIC --For each date without real price from source we calculate Forecast price as Average from last 3 months of available data
# MAGIC WITH ForecastDataset AS (
# MAGIC SELECT
# MAGIC   date_format(Dates.DateForJoin, 'yyyy-MM') as Date
# MAGIC   ,Dates.Mnemonic
# MAGIC   ,AVG(ForecastDataset1.Price) as Price
# MAGIC   ,'Y' as ForecastFlag
# MAGIC   ,MAX(ForecastDataset1.Source) as Source
# MAGIC FROM Dates
# MAGIC JOIN ForecastDataset1
# MAGIC ON ForecastDataset1.Mnemonic = Dates.Mnemonic
# MAGIC Where RowNumberForPriceFilter <=3
# MAGIC GROUP BY 
# MAGIC Dates.Mnemonic,
# MAGIC date_format(Dates.DateForJoin, 'yyyy-MM'),
# MAGIC ForecastDataset1.Mnemonic
# MAGIC )
# MAGIC 
# MAGIC ,SourceDataFiltered AS (
# MAGIC SELECT Mnemonic
# MAGIC ,Date
# MAGIC ,Price
# MAGIC ,null as CalculatedForecast
# MAGIC ,ForecastFlag
# MAGIC ,Source
# MAGIC --if Mnemonic exists in both Manual and any other file, we use NOT Manual source
# MAGIC ,ROW_NUMBER() OVER (PARTITION BY Mnemonic, Date ORDER BY CASE WHEN Source LIKE 'Manual%' THEN 2 ELSE 1 END) RowNumberForFilter
# MAGIC FROM SourceData
# MAGIC UNION
# MAGIC SELECT Mnemonic
# MAGIC ,Date
# MAGIC ,null as Price
# MAGIC ,Price as CalculatedForecast
# MAGIC ,ForecastFlag
# MAGIC ,Source
# MAGIC ,1 as RowNumberForFilter
# MAGIC FROM ForecastDataset
# MAGIC )
# MAGIC --If there are both quarterly and monthly rows, we duplicate quarterly price to all months in this quarter
# MAGIC ,CTE AS (
# MAGIC SELECT
# MAGIC    SourceData.Mnemonic
# MAGIC   ,SourceData.Date
# MAGIC   ,TO_DATE(NVL(LEFT(SourceData.Date, 4) || '-' || MonthToQuarterMapping.Month, SourceData.Date), 'yyyy-MM') AS DateForJoin
# MAGIC   ,SourceData.Price
# MAGIC   ,SourceData.CalculatedForecast
# MAGIC   ,SourceData.ForecastFlag
# MAGIC FROM SourceDataFiltered AS SourceData
# MAGIC LEFT JOIN MonthToQuarterMapping ON RIGHT(SourceData.Date, 2) = MonthToQuarterMapping.Quarter
# MAGIC WHERE SourceData.RowNumberForFilter = 1
# MAGIC )
# MAGIC --join dataset to Base Prices dataset (for each date we take base price that was active in this period)
# MAGIC SELECT 
# MAGIC    SourceData.Mnemonic
# MAGIC   ,SourceData.Date
# MAGIC   ,SourceData.DateForJoin
# MAGIC   ,SourceData.Price
# MAGIC   ,SourceData.CalculatedForecast
# MAGIC   ,IndexBasePrice.BasePrice
# MAGIC   ,IndexBasePrice.BasePriceUOM
# MAGIC   --,IndexBasePrice.PriceQ0
# MAGIC   --,IndexBasePrice.PriceQ0UOM
# MAGIC   ,SourceData.ForecastFlag
# MAGIC FROM CTE AS SourceData
# MAGIC LEFT JOIN BasePricesSource AS IndexBasePrice   ON IndexBasePrice.Supplier IS NULL 
# MAGIC                                               AND IndexBasePrice.MnemonicOrMaterialNumber = SourceData.Mnemonic
# MAGIC                                               AND SourceData.DateForJoin >= IndexBasePrice.EffectiveDate
# MAGIC                                               AND SourceData.DateForJoin <  IndexBasePrice.ExpiresDate

# COMMAND ----------

# MAGIC %sql 
# MAGIC  CREATE OR REPLACE TEMPORARY VIEW InputSourceStage0
# MAGIC AS
# MAGIC --Select distinct Proxy indexes from Proxy indexes tab (Some indexes do not have a forecast. However, we could use a their proxy index that does have a forecast)
# MAGIC WITH UnpivotedDataset AS (
# MAGIC SELECT NULLIF(ProxyMnemonics.`Material Nbr`, 'NaN')	          AS MaterialNumber
# MAGIC   ,NULLIF(ProxyMnemonics.Supplier, 'NaN')		              AS Supplier
# MAGIC   ,NULLIF(ProxyMnemonics.Mnemonic, 'NaN')		              AS Mnemonic
# MAGIC   ,NULLIF(ProxyMnemonics.ProxyMnemonic1, 'NaN')		          AS ProxyMnemonic
# MAGIC   ,NULLIF(`ProxyMnemonic1UOM`,'NaN')   					  AS ProxyMnemonicUOM
# MAGIC   ,1 AS ProxyNumber
# MAGIC FROM ProxyMnemonics
# MAGIC WHERE NULLIF(ProxyMnemonics.`Material Nbr`, 'NaN') IS NOT NULL AND NULLIF(ProxyMnemonics.ProxyMnemonic1, 'NaN') IS NOT NULL 
# MAGIC UNION 
# MAGIC SELECT NULLIF(ProxyMnemonics.`Material Nbr`, 'NaN')	          AS MaterialNumber
# MAGIC   ,NULLIF(ProxyMnemonics.Supplier, 'NaN')		              AS Supplier
# MAGIC   ,NULLIF(ProxyMnemonics.Mnemonic, 'NaN')		              AS Mnemonic
# MAGIC   ,NULLIF(ProxyMnemonics.ProxyMnemonic2, 'NaN')		          AS ProxyMnemonic
# MAGIC   ,NULLIF(`ProxyMnemonic2UOM`,'NaN')   				  AS ProxyMnemonicUOM
# MAGIC   ,2 AS ProxyNumber
# MAGIC FROM ProxyMnemonics
# MAGIC WHERE NULLIF(ProxyMnemonics.`Material Nbr`, 'NaN') IS NOT NULL AND NULLIF(ProxyMnemonics.ProxyMnemonic2, 'NaN') IS NOT NULL
# MAGIC UNION 
# MAGIC SELECT NULLIF(ProxyMnemonics.`Material Nbr`, 'NaN')	          AS MaterialNumber
# MAGIC   ,NULLIF(ProxyMnemonics.Supplier, 'NaN')		              AS Supplier
# MAGIC   ,NULLIF(ProxyMnemonics.Mnemonic, 'NaN')		              AS Mnemonic
# MAGIC   ,NULLIF(ProxyMnemonics.ProxyMnemonic3, 'NaN')		          AS ProxyMnemonic
# MAGIC   ,NULLIF(`ProxyMnemonic3UOM`,'NaN')   					  AS ProxyMnemonicUOM
# MAGIC   ,3 AS ProxyNumber
# MAGIC FROM ProxyMnemonics
# MAGIC WHERE NULLIF(ProxyMnemonics.`Material Nbr`, 'NaN') IS NOT NULL AND NULLIF(ProxyMnemonics.ProxyMnemonic3, 'NaN') IS NOT NULL ),
# MAGIC 
# MAGIC ProxySupplierSource AS (
# MAGIC SELECT Dates.DateForJoin,
# MAGIC        date_format(Dates.DateForJoin, 'yyyy-MM') as Date,
# MAGIC        Dates.Mnemonic, 
# MAGIC        ProxyMnemonics.MaterialNumber,  
# MAGIC        ProxyMnemonics.Supplier, 
# MAGIC        ProxyMnemonics.ProxyMnemonic, 
# MAGIC        ProxyMnemonics.ProxyMnemonicUOM as InputUOM,
# MAGIC        ProxyMnemonics.ProxyNumber,   
# MAGIC        ForecastDataset1.Date as PriceDate, 
# MAGIC        ForecastDataset1.Price,
# MAGIC        ROW_NUMBER() OVER (PARTITION BY Dates.Mnemonic, Dates.DateForJoin, ProxyMnemonics.MaterialNumber,  ProxyMnemonics.Supplier ORDER BY CASE WHEN ForecastDataset1.Price is null THEN 2 ELSE 1 END, ProxyNumber ) AS RowNumberForFilter
# MAGIC FROM Dates
# MAGIC JOIN UnpivotedDataset ProxyMnemonics ON ProxyMnemonics.Mnemonic = Dates.Mnemonic
# MAGIC LEFT JOIN ForecastDataset1 ON ProxyMnemonics.ProxyMnemonic = ForecastDataset1.Mnemonic
# MAGIC                                 AND Dates.DateForJoin = ForecastDataset1.DateForJoin ),
# MAGIC                     
# MAGIC ProxyMnemonicForecast AS (                                
# MAGIC SELECT Date,DateForJoin,
# MAGIC Mnemonic, MaterialNumber, Supplier, Price, CONCAT("Based on ",ProxyMnemonic) as ForecastSource, InputUOM
# MAGIC FROM ProxySupplierSource
# MAGIC WHERE RowNumberForFilter = 1 AND Price IS NOT NULL ),
# MAGIC 
# MAGIC --Select distinct indexes from Formula Configuration file and apply Avg Price configuration based on "InputN Avg (Month)" and "InputN Delay (Month)"
# MAGIC DistIndexes AS (
# MAGIC SELECT NULLIF(IndexFile.`Material Nbr`, 'NaN')	          AS MaterialNumber
# MAGIC   ,NULLIF(IndexFile.Supplier, 'NaN')		              AS Supplier
# MAGIC   ,NULLIF(IndexFile.Input1, 'NaN')		                  AS InputMnemonic
# MAGIC   ,CAST(REPLACE(`Input1 Avg (Month)`,'NaN','1') AS Int)   AS InputAvgMonths
# MAGIC   ,CAST(REPLACE(`Input1 Delay (Month)`,'NaN','0') AS Int) AS InputDelayMonths
# MAGIC FROM IndexFile
# MAGIC WHERE NULLIF(IndexFile.`Material Nbr`, 'NaN') IS NOT NULL AND NULLIF(IndexFile.Input1, 'NaN') IS NOT NULL 
# MAGIC UNION 
# MAGIC SELECT NULLIF(IndexFile.`Material Nbr`, 'NaN')	          AS MaterialNumber
# MAGIC   ,NULLIF(IndexFile.Supplier, 'NaN')		              AS Supplier
# MAGIC   ,NULLIF(IndexFile.Input2, 'NaN')		                  AS InputMnemonic
# MAGIC   ,CAST(REPLACE(`Input2 Avg (Month)`,'NaN','1') AS Int)   AS InputAvgMonths
# MAGIC   ,CAST(REPLACE(`Input2 Delay (Month)`,'NaN','0') AS Int) AS InputDelayMonths
# MAGIC FROM IndexFile
# MAGIC WHERE NULLIF(IndexFile.`Material Nbr`, 'NaN') IS NOT NULL AND NULLIF(IndexFile.Input2, 'NaN') IS NOT NULL
# MAGIC UNION 
# MAGIC SELECT NULLIF(IndexFile.`Material Nbr`, 'NaN')	          AS MaterialNumber
# MAGIC   ,NULLIF(IndexFile.Supplier, 'NaN')		              AS Supplier
# MAGIC   ,NULLIF(IndexFile.Input3, 'NaN')		                  AS InputMnemonic
# MAGIC   ,CAST(REPLACE(`Input3 Avg (Month)`,'NaN','1') AS Int)   AS InputAvgMonths
# MAGIC   ,CAST(REPLACE(`Input3 Delay (Month)`,'NaN','0') AS Int) AS InputDelayMonths
# MAGIC FROM IndexFile
# MAGIC WHERE NULLIF(IndexFile.`Material Nbr`, 'NaN') IS NOT NULL AND NULLIF(IndexFile.Input3, 'NaN') IS NOT NULL
# MAGIC UNION 
# MAGIC SELECT NULLIF(IndexFile.`Material Nbr`, 'NaN')	          AS MaterialNumber
# MAGIC   ,NULLIF(IndexFile.Supplier, 'NaN')		              AS Supplier
# MAGIC   ,NULLIF(IndexFile.Input4, 'NaN')		                  AS InputMnemonic
# MAGIC   ,CAST(REPLACE(`Input4 Avg (Month)`,'NaN','1') AS Int)   AS InputAvgMonths
# MAGIC   ,CAST(REPLACE(`Input4 Delay (Month)`,'NaN','0') AS Int) AS InputDelayMonths
# MAGIC FROM IndexFile
# MAGIC WHERE NULLIF(IndexFile.`Material Nbr`, 'NaN') IS NOT NULL AND NULLIF(IndexFile.Input4, 'NaN') IS NOT NULL
# MAGIC UNION 
# MAGIC SELECT NULLIF(IndexFile.`Material Nbr`, 'NaN')	          AS MaterialNumber
# MAGIC   ,NULLIF(IndexFile.Supplier, 'NaN')		              AS Supplier
# MAGIC   ,NULLIF(IndexFile.Input5, 'NaN')		                  AS InputMnemonic
# MAGIC   ,CAST(REPLACE(`Input5 Avg (Month)`,'NaN','1') AS Int)   AS InputAvgMonths
# MAGIC   ,CAST(REPLACE(`Input5 Delay (Month)`,'NaN','0') AS Int) AS InputDelayMonths
# MAGIC FROM IndexFile
# MAGIC WHERE NULLIF(IndexFile.`Material Nbr`, 'NaN') IS NOT NULL AND NULLIF(IndexFile.Input5, 'NaN') IS NOT NULL
# MAGIC ),
# MAGIC RangeIndexes AS
# MAGIC (
# MAGIC SELECT 
# MAGIC   DistIndexes.MaterialNumber
# MAGIC   ,DistIndexes.Supplier
# MAGIC   ,DistIndexes.InputMnemonic AS Mnemonic
# MAGIC   ,InputAvgMonths
# MAGIC   ,InputDelayMonths
# MAGIC   ,IndexData.Date AS Date
# MAGIC   ,IndexData.DateForJoin AS DateForJoin
# MAGIC   ,NULLIF(IndexData.Price, 'NaN') AS CurrentPrice
# MAGIC   ,IndexData.BasePrice
# MAGIC   ,IndexData.BasePriceUOM
# MAGIC   --,IndexDataHist.DateForJoin AS HistDate
# MAGIC   ,MIN(IndexDataHist.DateForJoin) AS PriceFromDate
# MAGIC   ,MAX(IndexDataHist.DateForJoin) AS PriceToDate
# MAGIC   ,AVG( coalesce(NULLIF(IndexDataHist.Price, 'NaN'),NULLIF(ProxyMnemonicForecast.Price, 'NaN'),NULLIF(IndexDataHist.CalculatedForecast, 'NaN'))) AS Price
# MAGIC   , MAX(IndexDataHist.ForecastFlag) AS ForecastFlag  
# MAGIC   , MAX( CASE WHEN IndexData.ForecastFlag = 'N'                   THEN ''
# MAGIC               WHEN NULLIF(IndexDataHist.Price, 'NaN') is not null THEN 'Index Source Data' 
# MAGIC                                                                   ELSE nvl(ProxyMnemonicForecast.ForecastSource,'Calculated') --get source of forecast
# MAGIC           END ) as ForecastSource
# MAGIC   ,COUNT(*) AS CountHistMonths
# MAGIC   ,ProxyMnemonicForecast.InputUOM
# MAGIC   
# MAGIC FROM DistIndexes
# MAGIC LEFT JOIN TransformedSource AS IndexData   ON DistIndexes.InputMnemonic = IndexData.Mnemonic
# MAGIC --join to calculate average prices based on "InputN Avg (Month)" and "InputN Delay (Month)"
# MAGIC LEFT JOIN TransformedSource AS IndexDataHist   ON DistIndexes.InputMnemonic = IndexDataHist.Mnemonic
# MAGIC                                               AND add_months(IndexData.DateForJoin, -1*(InputAvgMonths+InputDelayMonths)) < IndexDataHist.DateForJoin  
# MAGIC                                               AND add_months(IndexData.DateForJoin, -1*InputDelayMonths) >= IndexDataHist.DateForJoin                                
# MAGIC LEFT JOIN ProxyMnemonicForecast ON ProxyMnemonicForecast.MaterialNumber = DistIndexes.MaterialNumber
# MAGIC                                   AND ProxyMnemonicForecast.Supplier = DistIndexes.Supplier
# MAGIC                                   AND ProxyMnemonicForecast.Mnemonic = DistIndexes.InputMnemonic
# MAGIC                                   AND ProxyMnemonicForecast.DateForJoin = IndexDataHist.DateForJoin
# MAGIC GROUP BY 
# MAGIC DistIndexes.MaterialNumber
# MAGIC ,DistIndexes.Supplier
# MAGIC ,DistIndexes.InputMnemonic
# MAGIC ,InputAvgMonths
# MAGIC ,InputDelayMonths
# MAGIC ,IndexData.Date
# MAGIC ,IndexData.DateForJoin
# MAGIC ,NULLIF(IndexData.Price, 'NaN')
# MAGIC ,IndexData.BasePrice
# MAGIC ,IndexData.BasePriceUOM
# MAGIC ,ProxyMnemonicForecast.InputUOM
# MAGIC --WHERE 
# MAGIC --MaterialNumber = 'RM 30030' AND InputMnemonic = 'BZENAM2365'
# MAGIC --MaterialNumber = 'RM 40190'  AND InputMnemonic = 'BZENAM2365'
# MAGIC )
# MAGIC --Filter to remove perions which are not equal to required InputAvgMonths
# MAGIC SELECT * FROM RangeIndexes WHERE InputAvgMonths = CountHistMonths
# MAGIC --ORDER BY MaterialNumber,Mnemonic,Supplier,DateForJoin

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW InputSourceStage1
# MAGIC AS 
# MAGIC SELECT
# MAGIC    NULLIF(IndexFile.`Material Nbr`          , 'NaN')		 AS MaterialNumber
# MAGIC   ,NULLIF(IndexFile.`Material Description`  , 'NaN')		 AS MaterialDescription
# MAGIC   ,NULLIF(IndexFile.`Material Owner`        , 'NaN')		 AS MaterialOwner
# MAGIC   ,NULLIF(IndexFile.Supplier                , 'NaN')		 AS Supplier
# MAGIC   ,NULLIF(Input1Data.Date					, 'NaN')	     AS Date 
# MAGIC   ,CASE WHEN coalesce(NULLIF(Input1Data.ForecastFlag,'NaN'),'N')='Y' 
# MAGIC           OR coalesce(NULLIF(Input2Data.ForecastFlag,'NaN'),'N')='Y'
# MAGIC           OR coalesce(NULLIF(Input3Data.ForecastFlag,'NaN'),'N')='Y'
# MAGIC           OR coalesce(NULLIF(Input4Data.ForecastFlag,'NaN'),'N')='Y'
# MAGIC           OR coalesce(NULLIF(Input5Data.ForecastFlag,'NaN'),'N')='Y'
# MAGIC         THEN 'Y' 
# MAGIC        ELSE 'N' END                                          AS ForecastFlag--need to check all inputs 
# MAGIC    									 
# MAGIC   ,NULLIF(IndexFile.Input1                  , 'NaN')		 AS Input1Mnemonic
# MAGIC   ,CONCAT(Input1Data.PriceFromDate,' : ',last_day(Input1Data.PriceToDate),' (',Input1Data.InputAvgMonths,'/',Input1Data.InputDelayMonths,')') AS Input1MnemonicRange
# MAGIC   ,NULLIF(IndexFile.Input2                  , 'NaN')		 AS Input2Mnemonic
# MAGIC   ,CONCAT(Input2Data.PriceFromDate,' : ',last_day(Input2Data.PriceToDate),' (',Input2Data.InputAvgMonths,'/',Input2Data.InputDelayMonths,')') AS Input2MnemonicRange
# MAGIC   ,NULLIF(IndexFile.Input3                  , 'NaN')		 AS Input3Mnemonic
# MAGIC   ,CONCAT(Input3Data.PriceFromDate,' : ',last_day(Input3Data.PriceToDate),' (',Input3Data.InputAvgMonths,'/',Input3Data.InputDelayMonths,')') AS Input3MnemonicRange
# MAGIC   ,NULLIF(IndexFile.Input4                  , 'NaN')		 AS Input4Mnemonic
# MAGIC   ,CONCAT(Input4Data.PriceFromDate,' : ',last_day(Input4Data.PriceToDate),' (',Input4Data.InputAvgMonths,'/',Input4Data.InputDelayMonths,')') AS Input4MnemonicRange
# MAGIC   ,NULLIF(IndexFile.Input5                  , 'NaN')		 AS Input5Mnemonic
# MAGIC   ,CONCAT(Input5Data.PriceFromDate,' : ',last_day(Input5Data.PriceToDate),' (',Input5Data.InputAvgMonths,'/',Input5Data.InputDelayMonths,')') AS Input5MnemonicRange
# MAGIC   
# MAGIC ,CAST(NULLIF(Input1Data.Price, 'NaN') AS float)		     	 AS Input1
# MAGIC   ,coalesce(Input1Data.InputUOM,NULLIF(IndexFile.`Input1 UOM`            , 'NaN'))         AS Input1UOM
# MAGIC   ,Input1Data.BasePrice               		                 AS Input1BasePrice
# MAGIC   ,Input1Data.BasePriceUOM               		             AS Input1BasePriceUOM
# MAGIC   
# MAGIC   
# MAGIC   ,CAST(NULLIF(Input2Data.Price, 'NaN')  AS float)		     AS Input2
# MAGIC   ,coalesce(Input2Data.InputUOM,NULLIF(IndexFile.`Input2 UOM`            , 'NaN'))		 AS Input2UOM
# MAGIC   ,Input2Data.BasePrice               		                 AS Input2BasePrice
# MAGIC   ,Input2Data.BasePriceUOM               		             AS Input2BasePriceUOM
# MAGIC   
# MAGIC   ,CAST(NULLIF(Input3Data.Price, 'NaN')  AS float)		     AS Input3
# MAGIC   ,coalesce(Input3Data.InputUOM,NULLIF(IndexFile.`Input3 UOM`            , 'NaN'))		 AS Input3UOM
# MAGIC   ,Input3Data.BasePrice               		                 AS Input3BasePrice
# MAGIC   ,Input3Data.BasePriceUOM               		             AS Input3BasePriceUOM
# MAGIC   
# MAGIC   ,CAST(NULLIF(Input4Data.Price, 'NaN')  AS float)    		 AS Input4
# MAGIC   ,coalesce(Input4Data.InputUOM,NULLIF(IndexFile.`Input4 UOM`            , 'NaN'))		 AS Input4UOM
# MAGIC   ,Input4Data.BasePrice               		                 AS Input4BasePrice
# MAGIC   ,Input4Data.BasePriceUOM               		             AS Input4BasePriceUOM
# MAGIC   
# MAGIC   ,CAST(NULLIF(Input5Data.Price, 'NaN')  AS float)		     AS Input5
# MAGIC   ,coalesce(Input5Data.InputUOM,NULLIF(IndexFile.`Input5 UOM`            , 'NaN'))		 AS Input5UOM
# MAGIC   ,Input5Data.BasePrice               		                 AS Input5BasePrice
# MAGIC   ,Input5Data.BasePriceUOM               		             AS Input5BasePriceUOM
# MAGIC   
# MAGIC   ,NULLIF(IndexFile.Constant1               , 'NaN')		 AS Constant1
# MAGIC   ,NULLIF(IndexFile.`Constant1 UOM`         , 'NaN')		 AS Constant1UOM
# MAGIC   ,NULLIF(IndexFile.Constant2               , 'NaN')		 AS Constant2
# MAGIC   ,NULLIF(IndexFile.`Constant2 UOM`         , 'NaN')		 AS Constant2UOM
# MAGIC   ,NULLIF(IndexFile.Constant3               , 'NaN')		 AS Constant3
# MAGIC   ,NULLIF(IndexFile.`Constant3 UOM`         , 'NaN')		 AS Constant3UOM
# MAGIC   ,NULLIF(IndexFile.Constant4               , 'NaN')		 AS Constant4
# MAGIC   ,NULLIF(IndexFile.`Constant4 UOM`         , 'NaN')		 AS Constant4UOM
# MAGIC   ,NULLIF(IndexFile.Constant5               , 'NaN')		 AS Constant5
# MAGIC   ,NULLIF(IndexFile.`Constant5 UOM`         , 'NaN')		 AS Constant5UOM
# MAGIC   ,NULLIF(IndexFile.Constant6               , 'NaN')		 AS Constant6
# MAGIC   ,NULLIF(IndexFile.`Constant6 UOM`         , 'NaN')		 AS Constant6UOM
# MAGIC   ,IndexBasePrice.BasePrice                                  AS BasePrice
# MAGIC   ,IndexBasePrice.BasePriceUOM                               AS BasePriceUOM
# MAGIC   
# MAGIC   
# MAGIC   ,NULLIF(LOWER(IndexFile.Formula)||'     ' , 'NaN')		 AS Formula
# MAGIC   ,NULLIF(IndexFile.`Formula UOM`        , 'NaN') 		     AS FormulaUOM
# MAGIC   ,NULLIF(IndexFile.Quarterly               , 'NaN')		 AS Quarterly
# MAGIC FROM IndexFile
# MAGIC LEFT JOIN InputSourceStage0 Input1Data ON IndexFile.Input1 = Input1Data.Mnemonic 
# MAGIC                                           AND NULLIF(IndexFile.`Material Nbr`, 'NaN') = Input1Data.MaterialNumber
# MAGIC                                           AND NULLIF(IndexFile.Supplier, 'NaN') = Input1Data.Supplier
# MAGIC LEFT JOIN InputSourceStage0 Input2Data ON IndexFile.Input2 = Input2Data.Mnemonic AND Input1Data.Date = Input2Data.Date
# MAGIC                                           AND NULLIF(IndexFile.`Material Nbr`, 'NaN') = Input2Data.MaterialNumber
# MAGIC                                           AND NULLIF(IndexFile.Supplier, 'NaN') = Input2Data.Supplier
# MAGIC LEFT JOIN InputSourceStage0 Input3Data ON IndexFile.Input3 = Input3Data.Mnemonic AND Input1Data.Date = Input3Data.Date
# MAGIC                                           AND NULLIF(IndexFile.`Material Nbr`, 'NaN') = Input3Data.MaterialNumber
# MAGIC                                           AND NULLIF(IndexFile.Supplier, 'NaN') = Input3Data.Supplier
# MAGIC LEFT JOIN InputSourceStage0 Input4Data ON IndexFile.Input4 = Input4Data.Mnemonic AND Input1Data.Date = Input4Data.Date
# MAGIC                                           AND NULLIF(IndexFile.`Material Nbr`, 'NaN') = Input4Data.MaterialNumber
# MAGIC                                           AND NULLIF(IndexFile.Supplier, 'NaN') = Input4Data.Supplier
# MAGIC LEFT JOIN InputSourceStage0 Input5Data ON IndexFile.Input5 = Input5Data.Mnemonic AND Input1Data.Date = Input5Data.Date
# MAGIC                                           AND NULLIF(IndexFile.`Material Nbr`, 'NaN') = Input5Data.MaterialNumber
# MAGIC                                           AND NULLIF(IndexFile.Supplier, 'NaN') = Input5Data.Supplier
# MAGIC LEFT JOIN BasePricesSource AS IndexBasePrice   ON IndexBasePrice.Supplier =  IndexFile.Supplier
# MAGIC                                               AND IndexBasePrice.MnemonicOrMaterialNumber = IndexFile.`Material Nbr`
# MAGIC                                               AND Input1Data.DateForJoin >= IndexBasePrice.EffectiveDate
# MAGIC                                               AND Input1Data.DateForJoin <  IndexBasePrice.ExpiresDate

# COMMAND ----------

DFInputSource = sqlContext.sql("SELECT * FROM InputSourceStage1")
PDFInputSource = DFInputSource.fillna(0).select("*").toPandas()

# COMMAND ----------

#######################         UOM and Currency conversion           #############################
#If all UOMs and Currencies in the row are the same we leave them unchanged, otherwise convert all to USD/MT
for i in PDFInputSource.index: 
    DictVal = {} 
    # for each row collect Prices and splitted UOMs 
    for j in range(16,48,2): 
        key_col = PDFInputSource.columns[j]
        key_value = PDFInputSource[key_col][i]
        UOM_col = PDFInputSource.columns[j + 1] 
        UOM_non_splitted = "USD/MT" if PDFInputSource[UOM_col][i] is None else PDFInputSource[UOM_col][i]
        #print(("key_col: {} key_value: {} UOM_col: {}").format(PDFInputSource.columns[j],PDFInputSource[key_col][i],PDFInputSource[UOM_col][i])) #Test Step
        if key_value and UOM_non_splitted and str(UOM_non_splitted).lower() != "nan" and str(key_value) != "nan":
            UOM_splitted = UOM_non_splitted.split("/")
			# for values with only UOM or Currecy Code
            if len(UOM_splitted) == 1: 
                uom_key = UOM_splitted[0] 
                if uom_key in UOMDict.keys(): 
                    DictVal[key_col] = [key_value, None, uom_key.replace("MMBtu","MT")]
                    #print(("key_col: {} if uom_key in UOMDict.keys(): -> DictVal[key_col] : {}").format(key_col,DictVal[key_col]))
                else: 
                    DictVal[key_col] = [key_value, uom_key, None] 
                    #print(("key_col: {} else: -> DictVal[key_col] : {}").format(key_col,DictVal[key_col]))
            # for others we Create new Dictionary for each row
            elif len(UOM_splitted) == 2: 
                DictVal[key_col] = [key_value, UOM_splitted[0], UOM_splitted[1].replace("MMBtu","MT")]
                #print(("key_col: {} elif len(UOM_splitted) == 2 -> DictVal[key_col] : {}").format(key_col,DictVal[key_col]))
    if(DictVal):
      sample_value_from_dict = next( v for k, v in DictVal.items() if v)
      sample_cur = sample_value_from_dict[1] 
      sample_uom = sample_value_from_dict[2]
      #check if all UOMs are the same
      if any(((value[1] if value[1] else sample_cur) != sample_cur) or ((value[2] if value[2] else sample_uom) != sample_uom) for value in DictVal.values()): 
          for key, value in DictVal.items(): 
              if value[1] and value[1] != "USD":
                  try: 
                      value[0] *= CurrencyMappingPDf[value[1]][PDFInputSource['Date'][i]] 
                      value[1] = "USD" 
                  except KeyError: 
                      print(("No such Currency {} in Currency Mapping table.").format(value[1])) 
              if value[2] and value[2].lower() != "mt": 
                  try: 
                      value[0] = np.float32(float(value[0])/UOMDict[value[2]])
                      value[2] = "MT" 
                  except KeyError: 
                      print(("No such UOM {} in UOM Mapping table.").format(value[2]))
              PDFInputSource[key].at[i] = value[0] 
              cur = value[1] if value[1] else "" 
              uom = value[2] if value[2] else "" 
              #print(("cur: {} uom: {} value[1]: {} value[0]: {} value: {} ").format(cur,value[2], value[1],value[0],PDFInputSource[key].at[i] ))
              if cur and uom: 
                  PDFInputSource[key + "UOM"].at[i] = cur + "/" + uom 
              elif cur: 
                  PDFInputSource[key + "UOM"].at[i] = cur
              else: 
                  PDFInputSource[key + "UOM"].at[i] = uom

# COMMAND ----------

#testDF = PDFInputSource.loc[PDFInputSource['MaterialNumber'] == "OLOA 2509M"] RM 40190
#print(testDF.loc[:,'Input1':'Constant4'])

IndexSchema2 = StructType([
       StructField("MaterialNumber",StringType(),True),
   StructField("MaterialDescription",StringType(),True),
   StructField("MaterialOwner",StringType(),True),
   StructField("Supplier",StringType(),True),
   StructField("Date",StringType(),True),
   StructField("ForecastFlag",StringType(),True),
   StructField("Input1Mnemonic",StringType(),True),
   StructField("Input1MnemonicRange",StringType(),True),
   StructField("Input2Mnemonic",StringType(),True),
   StructField("Input2MnemonicRange",StringType(),True),
   StructField("Input3Mnemonic",StringType(),True),
   StructField("Input3MnemonicRange",StringType(),True),
   StructField("Input4Mnemonic",StringType(),True),
   StructField("Input4MnemonicRange",StringType(),True),
   StructField("Input5Mnemonic",StringType(),True),
   StructField("Input5MnemonicRange",StringType(),True),
   StructField("Input1",FloatType(),True),
   StructField("Input1UOM",StringType(),True),
   StructField("Input1BasePrice",FloatType(),True),
   StructField("Input1BasePriceUOM",StringType(),True),
   StructField("Input2",FloatType(),True),
   StructField("Input2UOM",StringType(),True),
   StructField("Input2BasePrice",FloatType(),True),
   StructField("Input2BasePriceUOM",StringType(),True),
   StructField("Input3",FloatType(),True),
   StructField("Input3UOM",StringType(),True),
   StructField("Input3BasePrice",FloatType(),True),
   StructField("Input3BasePriceUOM",StringType(),True),
   StructField("Input4",FloatType(),True),
   StructField("Input4UOM",StringType(),True),
   StructField("Input4BasePrice",FloatType(),True),
   StructField("Input4BasePriceUOM",StringType(),True),
   StructField("Input5",FloatType(),True),
   StructField("Input5UOM",StringType(),True),
   StructField("Input5BasePrice",FloatType(),True),
   StructField("Input5BasePriceUOM",StringType(),True),
   StructField("Constant1",FloatType(),True),
   StructField("Constant1UOM",StringType(),True),
   StructField("Constant2",FloatType(),True),
   StructField("Constant2UOM",StringType(),True),
   StructField("Constant3",FloatType(),True),
   StructField("Constant3UOM",StringType(),True),
   StructField("Constant4",FloatType(),True),
   StructField("Constant4UOM",StringType(),True),
   StructField("Constant5",FloatType(),True),
   StructField("Constant5UOM",StringType(),True),
   StructField("Constant6",FloatType(),True),
   StructField("Constant6UOM",StringType(),True),
   StructField("BasePrice",FloatType(),True),
   StructField("BasePriceUOM",StringType(),True),
   StructField("Formula",StringType(),True),
   StructField("FormulaUOM",StringType(),True),
   StructField("Quarterly",StringType(),True)
                         ])

# COMMAND ----------

SparkInputSourceDF = spark.createDataFrame(PDFInputSource, schema=IndexSchema2)
SparkInputSourceDF.registerTempTable("InputSourceStage2")

# COMMAND ----------

# MAGIC %sql
# MAGIC --Quarter Index Calculation (for formulas whe Quarterly flag = Y)
# MAGIC CREATE OR REPLACE TEMPORARY VIEW InputSourceQ
# MAGIC AS 
# MAGIC WITH InputSourceQ AS (
# MAGIC SELECT
# MAGIC    InputSource.MaterialNumber
# MAGIC   ,InputSource.Supplier
# MAGIC   ,InputSource.Date
# MAGIC   ,CAST(LEFT(InputSource.Date, 4) AS INT) AS Year
# MAGIC   ,MonthToQuarterMapping.Quarter
# MAGIC   ,(InputSource.Input1)			AS Input1
# MAGIC   ,(InputSource.Input2)			AS Input2
# MAGIC   ,(InputSource.Input3)			AS Input3
# MAGIC   ,(InputSource.Input4)			AS Input4
# MAGIC   ,(InputSource.Input5)			AS Input5
# MAGIC   ,ROW_NUMBER() OVER(PARTITION BY InputSource.MaterialNumber,InputSource.Supplier,CAST(LEFT(InputSource.Date, 4) AS INT),MonthToQuarterMapping.Quarter 
# MAGIC                       ORDER BY CAST(RIGHT(InputSource.Date, 2) AS INT)) AS FirstMonthOfQuarterFilter
# MAGIC FROM InputSourceStage2 InputSource
# MAGIC INNER JOIN MonthToQuarterMapping ON RIGHT(InputSource.Date, 2) = MonthToQuarterMapping.Month
# MAGIC WHERE Quarterly = 'Y'
# MAGIC )
# MAGIC SELECT InputSourceQ.MaterialNumber
# MAGIC   ,InputSourceQ.Supplier
# MAGIC   ,InputSourceQ.Year || '-' || MonthToQuarterMapping.Month AS Date
# MAGIC   ,InputSourceQ.Quarter
# MAGIC   ,InputSourceQ.Input1
# MAGIC   ,InputSourceQ.Input2
# MAGIC   ,InputSourceQ.Input3
# MAGIC   ,InputSourceQ.Input4
# MAGIC   ,InputSourceQ.Input5
# MAGIC FROM InputSourceQ
# MAGIC JOIN MonthToQuarterMapping ON InputSourceQ.Quarter = MonthToQuarterMapping.Quarter
# MAGIC WHERE FirstMonthOfQuarterFilter = 1
# MAGIC --ORDER BY MaterialNumber, Supplier, Date

# COMMAND ----------

# MAGIC %sql
# MAGIC --Build final dataset in Formula UOM
# MAGIC CREATE OR REPLACE TEMPORARY VIEW InputSource
# MAGIC AS 
# MAGIC SELECT
# MAGIC    InputSourceStage2.MaterialNumber
# MAGIC   ,InputSourceStage2.MaterialDescription
# MAGIC   ,InputSourceStage2.MaterialOwner
# MAGIC   ,InputSourceStage2.Supplier
# MAGIC   ,InputSourceStage2.Date 
# MAGIC   ,InputSourceStage2.ForecastFlag
# MAGIC    									 
# MAGIC   ,InputSourceStage2.Input1Mnemonic
# MAGIC   ,Input1MnemonicRange
# MAGIC   ,InputSourceStage2.Input2Mnemonic
# MAGIC   ,Input2MnemonicRange
# MAGIC   ,InputSourceStage2.Input3Mnemonic
# MAGIC   ,Input3MnemonicRange
# MAGIC   ,InputSourceStage2.Input4Mnemonic
# MAGIC   ,Input4MnemonicRange
# MAGIC   ,InputSourceStage2.Input5Mnemonic
# MAGIC   ,Input5MnemonicRange
# MAGIC    										
# MAGIC   ,InputSourceQ.Input1
# MAGIC   ,InputSourceStage2.Input1UOM
# MAGIC   ,InputSourceStage2.Input1BasePrice
# MAGIC   ,InputSourceStage2.Input1BasePriceUOM
# MAGIC   
# MAGIC   ,InputSourceQ.Input2
# MAGIC   ,InputSourceStage2.Input2UOM
# MAGIC   ,InputSourceStage2.Input2BasePrice
# MAGIC   ,InputSourceStage2.Input2BasePriceUOM
# MAGIC   
# MAGIC   ,InputSourceQ.Input3
# MAGIC   ,InputSourceStage2.Input3UOM
# MAGIC   ,InputSourceStage2.Input3BasePrice
# MAGIC   ,InputSourceStage2.Input3BasePriceUOM
# MAGIC   
# MAGIC   ,InputSourceQ.Input4
# MAGIC   ,InputSourceStage2.Input4UOM
# MAGIC   ,InputSourceStage2.Input4BasePrice
# MAGIC   ,InputSourceStage2.Input4BasePriceUOM
# MAGIC   ,InputSourceQ.Input5
# MAGIC   ,InputSourceStage2.Input5UOM
# MAGIC   ,InputSourceStage2.Input5BasePrice
# MAGIC   ,InputSourceStage2.Input5BasePriceUOM
# MAGIC   
# MAGIC   ,InputSourceStage2.Constant1
# MAGIC   ,InputSourceStage2.Constant1UOM
# MAGIC   ,InputSourceStage2.Constant2
# MAGIC   ,InputSourceStage2.Constant2UOM
# MAGIC   ,InputSourceStage2.Constant3
# MAGIC   ,InputSourceStage2.Constant3UOM
# MAGIC   ,InputSourceStage2.Constant4
# MAGIC   ,InputSourceStage2.Constant4UOM
# MAGIC   ,InputSourceStage2.Constant5
# MAGIC   ,InputSourceStage2.Constant5UOM
# MAGIC   ,InputSourceStage2.Constant6
# MAGIC   ,InputSourceStage2.Constant6UOM
# MAGIC   ,InputSourceStage2.BasePrice
# MAGIC   ,InputSourceStage2.BasePriceUOM
# MAGIC   
# MAGIC   ,InputSourceStage2.Formula
# MAGIC   ,InputSourceStage2.FormulaUOM
# MAGIC   ,InputSourceStage2.Quarterly
# MAGIC 
# MAGIC FROM InputSourceStage2
# MAGIC LEFT JOIN InputSourceQ ON InputSourceStage2.MaterialNumber   = InputSourceQ.MaterialNumber
# MAGIC                               AND InputSourceStage2.Supplier = InputSourceQ.Supplier
# MAGIC                               AND InputSourceStage2.Date     = InputSourceQ.Date
# MAGIC WHERE InputSourceStage2.Quarterly = 'Y'
# MAGIC 
# MAGIC UNION ALL
# MAGIC 
# MAGIC SELECT
# MAGIC    InputSourceStage2.MaterialNumber
# MAGIC   ,InputSourceStage2.MaterialDescription
# MAGIC   ,InputSourceStage2.MaterialOwner
# MAGIC   ,InputSourceStage2.Supplier
# MAGIC   ,InputSourceStage2.Date 
# MAGIC   ,InputSourceStage2.ForecastFlag
# MAGIC    									 
# MAGIC   ,InputSourceStage2.Input1Mnemonic
# MAGIC   ,Input1MnemonicRange
# MAGIC   ,InputSourceStage2.Input2Mnemonic
# MAGIC   ,Input2MnemonicRange
# MAGIC   ,InputSourceStage2.Input3Mnemonic
# MAGIC   ,Input3MnemonicRange
# MAGIC   ,InputSourceStage2.Input4Mnemonic
# MAGIC   ,Input4MnemonicRange
# MAGIC   ,InputSourceStage2.Input5Mnemonic
# MAGIC   ,Input5MnemonicRange
# MAGIC    										
# MAGIC   ,InputSourceStage2.Input1
# MAGIC   ,InputSourceStage2.Input1UOM
# MAGIC   ,InputSourceStage2.Input1BasePrice
# MAGIC   ,InputSourceStage2.Input1BasePriceUOM
# MAGIC  
# MAGIC   ,InputSourceStage2.Input2
# MAGIC   ,InputSourceStage2.Input2UOM
# MAGIC   ,InputSourceStage2.Input2BasePrice
# MAGIC   ,InputSourceStage2.Input2BasePriceUOM
# MAGIC  
# MAGIC   ,InputSourceStage2.Input3
# MAGIC   ,InputSourceStage2.Input3UOM
# MAGIC   ,InputSourceStage2.Input3BasePrice
# MAGIC   ,InputSourceStage2.Input3BasePriceUOM
# MAGIC   
# MAGIC   ,InputSourceStage2.Input4
# MAGIC   ,InputSourceStage2.Input4UOM
# MAGIC   ,InputSourceStage2.Input4BasePrice
# MAGIC   ,InputSourceStage2.Input4BasePriceUOM
# MAGIC   
# MAGIC   ,InputSourceStage2.Input5
# MAGIC   ,InputSourceStage2.Input5UOM
# MAGIC   ,InputSourceStage2.Input5BasePrice
# MAGIC   ,InputSourceStage2.Input5BasePriceUOM
# MAGIC   
# MAGIC   ,InputSourceStage2.Constant1
# MAGIC   ,InputSourceStage2.Constant1UOM
# MAGIC   ,InputSourceStage2.Constant2
# MAGIC   ,InputSourceStage2.Constant2UOM
# MAGIC   ,InputSourceStage2.Constant3
# MAGIC   ,InputSourceStage2.Constant3UOM
# MAGIC   ,InputSourceStage2.Constant4
# MAGIC   ,InputSourceStage2.Constant4UOM
# MAGIC   ,InputSourceStage2.Constant5
# MAGIC   ,InputSourceStage2.Constant5UOM
# MAGIC   ,InputSourceStage2.Constant6
# MAGIC   ,InputSourceStage2.Constant6UOM
# MAGIC   ,InputSourceStage2.BasePrice
# MAGIC   ,InputSourceStage2.BasePriceUOM
# MAGIC   
# MAGIC   ,InputSourceStage2.Formula
# MAGIC   ,InputSourceStage2.FormulaUOM
# MAGIC   ,InputSourceStage2.Quarterly
# MAGIC 
# MAGIC FROM InputSourceStage2
# MAGIC WHERE InputSourceStage2.Quarterly = 'N'

# COMMAND ----------

FinalDataset = sqlContext.sql("SELECT * FROM InputSource")
#Form columns list in format ('ColumnName1', `ColumnName1`,'ColumnName2' ...) - `ColumnName1` would be replaced with their values
ColumnListWithVal = ["'" + col + "', `" + col + "`" for col in FinalDataset.columns]
ColumnListWithValStr = ','.join(ColumnListWithVal)
#Form columns list in format ('ColumnName1'|'ColumnName2'|...)
ColumnListStr = '|'.join(FinalDataset.columns)
# Get column names with reir values in one column, column list in other and our finale dataset 
BaseDataset = sqlContext.sql("SELECT map({}) AS ColumnListWithValues, '{}' AS ColumnList, * FROM InputSource".format(ColumnListWithValStr, ColumnListStr))
BaseDataset.registerTempTable("BaseDataset")

# COMMAND ----------

import re

def CalculateFormula(FormulaString, ColumnsString, ColumnStruct):
  
  ColumnList = ColumnsString.split('|')

  for item in ColumnList:
    itemCleansed = item.replace('[', "\[").replace(']', "\]")

    try:
      StartIndex = re.search(itemCleansed + '([^a-z^\[])', FormulaString, flags=re.I).start()
    except:
      StartIndex = -1
    
    #print(item + "     " + str(StartIndex))
    while StartIndex > -1:
      FormulaString = FormulaString[:StartIndex] + str(ColumnStruct[item]) + FormulaString[StartIndex+len(item):]

      #print(FormulaString)
      try:
        StartIndex = re.search(itemCleansed + '([^a-z^\[])', FormulaString, flags=re.I).start()
      except:
        break
    
  
  try:
    return eval(FormulaString)
  except:
    return ''
      #return Value in Struct.keys()

spark.udf.register("CalculateFormula", CalculateFormula)

# COMMAND ----------

# MAGIC %sql
# MAGIC --#######################         Calculate Formalas            #############################
# MAGIC CREATE OR REPLACE TEMPORARY VIEW FormulaResults AS 
# MAGIC SELECT 
# MAGIC    BaseDataset.MaterialNumber
# MAGIC   ,BaseDataset.MaterialDescription
# MAGIC   ,BaseDataset.MaterialOwner
# MAGIC   ,BaseDataset.Supplier
# MAGIC   ,BaseDataset.Formula
# MAGIC   ,CalculateFormula(`Formula`, ColumnList, ColumnListWithValues)  / nvl(SourceUOMMapping.Ratio,1) * nvl(CurrencyMapping.Rate,1)  AS MaterialPriceStandartUOM
# MAGIC   ,CalculateFormula(`Formula`, ColumnList, ColumnListWithValues)  AS MaterialPrice
# MAGIC   ,nvl(upper(BaseDataset.Input1UOM),'USD/MT') AS FormulaUOM
# MAGIC   ,BaseDataset.Date
# MAGIC   ,BaseDataset.ForecastFlag
# MAGIC   ,BaseDataset.Quarterly
# MAGIC FROM BaseDataset
# MAGIC LEFT JOIN UOMMapping SourceUOMMapping   ON 'mt' = lower(SourceUOMMapping.TargetUOM) 
# MAGIC                                                   AND RIGHT(lower(BaseDataset.Input1UOM), length(BaseDataset.Input1UOM) - instr(BaseDataset.Input1UOM,'/'))= lower(SourceUOMMapping.UOM)
# MAGIC LEFT JOIN CurrencyMapping    ON LEFT(lower(BaseDataset.Input1UOM),instr(BaseDataset.Input1UOM,'/')-1) <> 'usd'
# MAGIC                                                   AND LEFT(lower(BaseDataset.Input1UOM),instr(BaseDataset.Input1UOM,'/')-1) = lower(CurrencyMapping.TargetCurrency)
# MAGIC                                                   AND BaseDataset.Date = CurrencyMapping.Date

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW MaterialResultWithIndexes AS 
# MAGIC WITH CTE as (
# MAGIC SELECT 
# MAGIC    FormulaResults.MaterialNumber
# MAGIC   ,FormulaResults.MaterialDescription
# MAGIC   ,FormulaResults.MaterialOwner
# MAGIC   ,FormulaResults.Supplier
# MAGIC   ,InputSource.Input1Mnemonic AS IndexMnemonic
# MAGIC   ,1 AS InputNumber
# MAGIC   ,InputSource.Input1MnemonicRange AS IndexRange
# MAGIC   ,NULLIF(InputSource.Input1, 'NaN') AS IndexPrice
# MAGIC   ,nvl(InputSource.Input1UOM,"USD/MT") AS IndexUOM
# MAGIC   ,FormulaResults.Date
# MAGIC   ,FormulaResults.MaterialPriceStandartUOM
# MAGIC   ,FormulaResults.MaterialPrice
# MAGIC   ,FormulaResults.FormulaUOM
# MAGIC   ,FormulaResults.ForecastFlag
# MAGIC   ,FormulaResults.Quarterly
# MAGIC   ,FormulaResults.Formula
# MAGIC FROM FormulaResults
# MAGIC LEFT JOIN InputSource ON FormulaResults.MaterialNumber   = InputSource.MaterialNumber
# MAGIC                               AND FormulaResults.Supplier = InputSource.Supplier
# MAGIC                               AND FormulaResults.Date     = InputSource.Date
# MAGIC WHERE NVL(Input1Mnemonic, 'NaN') <> 'NaN'
# MAGIC 
# MAGIC UNION 
# MAGIC 
# MAGIC SELECT 
# MAGIC    FormulaResults.MaterialNumber
# MAGIC   ,FormulaResults.MaterialDescription
# MAGIC   ,FormulaResults.MaterialOwner
# MAGIC   ,FormulaResults.Supplier
# MAGIC   ,InputSource.Input2Mnemonic AS IndexMnemonic
# MAGIC   ,1 AS InputNumber
# MAGIC   ,InputSource.Input2MnemonicRange AS IndexRange
# MAGIC   ,NULLIF(InputSource.Input2, 'NaN') AS IndexPrice
# MAGIC   ,nvl(InputSource.Input2UOM,"USD/MT") AS IndexUOM
# MAGIC   ,FormulaResults.Date
# MAGIC   ,FormulaResults.MaterialPriceStandartUOM
# MAGIC   ,FormulaResults.MaterialPrice
# MAGIC   ,FormulaResults.FormulaUOM
# MAGIC   ,FormulaResults.ForecastFlag
# MAGIC   ,FormulaResults.Quarterly
# MAGIC   ,FormulaResults.Formula
# MAGIC FROM FormulaResults
# MAGIC LEFT JOIN InputSource ON FormulaResults.MaterialNumber    = InputSource.MaterialNumber
# MAGIC                               AND FormulaResults.Supplier = InputSource.Supplier
# MAGIC                               AND FormulaResults.Date     = InputSource.Date
# MAGIC WHERE NVL(Input2Mnemonic, 'NaN') <> 'NaN'
# MAGIC 
# MAGIC UNION 
# MAGIC 
# MAGIC SELECT 
# MAGIC    FormulaResults.MaterialNumber
# MAGIC   ,FormulaResults.MaterialDescription
# MAGIC   ,FormulaResults.MaterialOwner
# MAGIC   ,FormulaResults.Supplier
# MAGIC   ,InputSource.Input3Mnemonic AS IndexMnemonic
# MAGIC   ,3 AS InputNumber
# MAGIC   ,InputSource.Input3MnemonicRange AS IndexRange
# MAGIC   ,NULLIF(InputSource.Input3, 'NaN') AS IndexPrice
# MAGIC   ,nvl(InputSource.Input3UOM,"USD/MT") AS IndexUOM
# MAGIC   ,FormulaResults.Date
# MAGIC   ,FormulaResults.MaterialPriceStandartUOM
# MAGIC   ,FormulaResults.MaterialPrice
# MAGIC   ,FormulaResults.FormulaUOM
# MAGIC   ,FormulaResults.ForecastFlag
# MAGIC   ,FormulaResults.Quarterly
# MAGIC   ,FormulaResults.Formula
# MAGIC FROM FormulaResults
# MAGIC LEFT JOIN InputSource ON FormulaResults.MaterialNumber    = InputSource.MaterialNumber
# MAGIC                               AND FormulaResults.Supplier = InputSource.Supplier
# MAGIC                               AND FormulaResults.Date     = InputSource.Date
# MAGIC WHERE NVL(Input3Mnemonic, 'NaN') <> 'NaN'
# MAGIC 
# MAGIC UNION 
# MAGIC 
# MAGIC SELECT 
# MAGIC    FormulaResults.MaterialNumber
# MAGIC   ,FormulaResults.MaterialDescription
# MAGIC   ,FormulaResults.MaterialOwner
# MAGIC   ,FormulaResults.Supplier
# MAGIC   ,InputSource.Input4Mnemonic AS IndexMnemonic
# MAGIC   ,4 AS InputNumber
# MAGIC   ,InputSource.Input4MnemonicRange AS IndexRange
# MAGIC   ,NULLIF(InputSource.Input4, 'NaN') AS IndexPrice
# MAGIC   ,nvl(InputSource.Input4UOM,"USD/MT") AS IndexUOM
# MAGIC   ,FormulaResults.Date
# MAGIC   ,FormulaResults.MaterialPriceStandartUOM
# MAGIC   ,FormulaResults.MaterialPrice
# MAGIC   ,FormulaResults.FormulaUOM
# MAGIC   ,FormulaResults.ForecastFlag
# MAGIC   ,FormulaResults.Quarterly
# MAGIC   ,FormulaResults.Formula
# MAGIC FROM FormulaResults
# MAGIC LEFT JOIN InputSource ON FormulaResults.MaterialNumber    = InputSource.MaterialNumber
# MAGIC                               AND FormulaResults.Supplier = InputSource.Supplier
# MAGIC                               AND FormulaResults.Date     = InputSource.Date
# MAGIC WHERE NVL(Input4Mnemonic, 'NaN') <> 'NaN'
# MAGIC 
# MAGIC UNION 
# MAGIC 
# MAGIC SELECT 
# MAGIC    FormulaResults.MaterialNumber
# MAGIC   ,FormulaResults.MaterialDescription
# MAGIC   ,FormulaResults.MaterialOwner
# MAGIC   ,FormulaResults.Supplier
# MAGIC   ,InputSource.Input5Mnemonic AS IndexMnemonic
# MAGIC   ,5 AS InputNumber
# MAGIC   ,InputSource.Input5MnemonicRange AS IndexRange
# MAGIC   ,NULLIF(InputSource.Input5, 'NaN') AS IndexPrice
# MAGIC   ,nvl(InputSource.Input5UOM,"USD/MT") AS IndexUOM
# MAGIC   ,FormulaResults.Date
# MAGIC   ,FormulaResults.MaterialPriceStandartUOM
# MAGIC   ,FormulaResults.MaterialPrice
# MAGIC   ,FormulaResults.FormulaUOM
# MAGIC   ,FormulaResults.ForecastFlag
# MAGIC   ,FormulaResults.Quarterly
# MAGIC   ,FormulaResults.Formula
# MAGIC FROM FormulaResults
# MAGIC LEFT JOIN InputSource ON FormulaResults.MaterialNumber    = InputSource.MaterialNumber
# MAGIC                               AND FormulaResults.Supplier = InputSource.Supplier
# MAGIC                               AND FormulaResults.Date     = InputSource.Date
# MAGIC WHERE NVL(Input5Mnemonic, 'NaN') <> 'NaN')
# MAGIC Select 
# MAGIC    CTE.MaterialNumber
# MAGIC   ,CTE.MaterialDescription
# MAGIC   ,CTE.MaterialOwner
# MAGIC   ,CTE.Supplier
# MAGIC   ,CTE.IndexMnemonic
# MAGIC   ,CTE.InputNumber
# MAGIC   ,CTE.IndexRange
# MAGIC   ,CTE.IndexPrice
# MAGIC   ,CTE.IndexPrice / nvl(SourceUOMMapping.Ratio,1) * nvl(CurrencyMapping.Rate,1)  AS IndexPriceStandartUOM
# MAGIC   ,CTE.IndexUOM
# MAGIC   ,CTE.Date
# MAGIC   ,CTE.MaterialPriceStandartUOM
# MAGIC   ,CTE.MaterialPrice
# MAGIC   ,CTE.FormulaUOM
# MAGIC   ,CTE.ForecastFlag
# MAGIC   ,CTE.Quarterly
# MAGIC   ,CTE.Formula
# MAGIC FROM CTE
# MAGIC LEFT JOIN UOMMapping SourceUOMMapping   ON 'mt' = lower(SourceUOMMapping.TargetUOM) 
# MAGIC                                                   AND RIGHT(lower(CTE.IndexUOM), length(CTE.IndexUOM) - instr(CTE.IndexUOM,'/'))= lower(SourceUOMMapping.UOM)
# MAGIC LEFT JOIN CurrencyMapping    ON LEFT(lower(CTE.IndexUOM),instr(CTE.IndexUOM,'/')-1) <> 'usd'
# MAGIC                                                   AND LEFT(lower(CTE.IndexUOM),instr(CTE.IndexUOM,'/')-1) = lower(CurrencyMapping.TargetCurrency)
# MAGIC                                                   AND CTE.Date = CurrencyMapping.Date

# COMMAND ----------

try:
  DFFormulaRes = sqlContext.sql("SELECT * FROM MaterialResultWithIndexes")
except:
  print("Exception occurred 1166")
if DFFormulaRes.count() == 0:
  print("No data rows 1166")
else:
  DFFormulaRes.coalesce(1).write.format("csv").options(header='true').mode("overwrite").save("dbfs:/mnt/Produced/PBIOroniteForecasting/MaterialResultWithIndexes/")
