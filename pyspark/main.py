# Databricks notebook source
# MAGIC %md
# MAGIC <h2>ICS 613-50 Group Project</h2>
# MAGIC Sparks - Question 1 - 3<br/>
# MAGIC Prakat Tuladhar & Seong Kang

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Set Up</strong>:
# MAGIC - Create a directory for the files
# MAGIC - Upload all the files into directory
# MAGIC - Load all files into RDDs
# MAGIC - Convert all files into DFs

# COMMAND ----------

dbutils.fs.mkdirs("dbfs:/FileStore/tables/group-project/")

# COMMAND ----------

covidRdd = sc.textFile("dbfs:/FileStore/tables/group-project/weekly_covid.csv")
aaplRdd = sc.textFile("dbfs:/FileStore/tables/group-project/AAPL.csv")
metaRdd = sc.textFile("dbfs:/FileStore/tables/group-project/META.csv")
googRdd = sc.textFile("dbfs:/FileStore/tables/group-project/GOOG.csv")

robinAaplRdd = sc.textFile("dbfs:/FileStore/tables/group-project/robin_AAPL.csv")
robinMetaRdd = sc.textFile("dbfs:/FileStore/tables/group-project/robin_FB.csv")
robinGoogRdd = sc.textFile("dbfs:/FileStore/tables/group-project/robin_GOOG.csv")

covidRddHeader = covidRdd.first()
aaplRddHeader = aaplRdd.first()
metaRddHeader = metaRdd.first()
googRddHeader = googRdd.first()
robinAaplHeader = robinAaplRdd.first()
robinMetaHeader = robinMetaRdd.first()
robinGoogHeader = robinGoogRdd.first()

covidRdd = covidRdd.filter(lambda line: line != covidRddHeader)
aaplRdd = aaplRdd.filter(lambda line: line != aaplRddHeader)
metaRdd = metaRdd.filter(lambda line: line != metaRddHeader)
googRdd = googRdd.filter(lambda line: line != googRddHeader)
robinAaplRdd = robinAaplRdd.filter(lambda line: line != robinAaplHeader)
robinMetaRdd = robinMetaRdd.filter(lambda line: line != robinMetaHeader)
robinGoogRdd = robinGoogRdd.filter(lambda line: line != robinGoogHeader)

covidRdd = covidRdd.map(lambda line: line.split(','))
aaplRdd = aaplRdd.map(lambda line: line.split(','))
metaRdd = metaRdd.map(lambda line: line.split(','))
googRdd = googRdd.map(lambda line: line.split(','))
robinAaplRdd = robinAaplRdd.map(lambda line: line.split(','))
robinMetaRdd = robinMetaRdd.map(lambda line: line.split(','))
robinGoogRdd = robinGoogRdd.map(lambda line: line.split(','))

# COMMAND ----------

from pyspark.sql.types import TimestampType, IntegerType

covidDf = covidRdd.toDF(['geography','date','weeklyCases','historicCases'])
covidDf = covidDf.select(['date', 'weeklyCases'])
aaplDf = aaplRdd.toDF(['date','open','high','low','close','adjClose','volume'])
metaDf = metaRdd.toDF(['date','open','high','low','close','adjClose','volume'])
googDf = googRdd.toDF(['date','open','high','low','close','adjClose','volume'])
robinAaplDf = robinAaplRdd.toDF(['date', 'holders'])
robinMetaDf = robinMetaRdd.toDF(['date', 'holders'])
robinGoogDf = robinGoogRdd.toDF(['date', 'holders'])

# COMMAND ----------

covidDf.limit(5).show()
aaplDf.limit(5).show()
robinGoogDf.limit(5).show()

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Question 1:</strong> Combine the datasets for different files into a single file for Stock price database and Robinhood popularity stock.

# COMMAND ----------

from pyspark.sql import functions as F
import matplotlib.pyplot as plt
import numpy as np

robinAaplDf = robinAaplDf.withColumn('date', F.regexp_replace('date', '"', ''))
robinAaplDf = robinAaplDf.withColumn("date", F.to_timestamp(F.col("date")))
robinAaplDateDf = robinAaplDf.select(F.date_format('date','yyyy-MM-dd').alias('date'), 'holders').groupby('date').agg(F.round(F.avg('holders'))).withColumnRenamed('round(avg(holders), 0)', 'robinhoodAaplHolders')

robinMetaDf = robinMetaDf.withColumn('date', F.regexp_replace('date', '"', ''))
robinMetaDf = robinMetaDf.withColumn("date", F.to_timestamp(F.col("date")))
robinMetaDateDf = robinMetaDf.select(F.date_format('date','yyyy-MM-dd').alias('date'), 'holders').groupby('date').agg(F.round(F.avg('holders'))).withColumnRenamed('round(avg(holders), 0)', 'robinhoodMetaHolders')

robinGoogDf = robinGoogDf.withColumn('date', F.regexp_replace('date', '"', ''))
robinGoogDf = robinGoogDf.withColumn("date", F.to_timestamp(F.col("date")))
robinGoogDateDf = robinGoogDf.select(F.date_format('date','yyyy-MM-dd').alias('date'), 'holders').groupby('date').agg(F.round(F.avg('holders'))).withColumnRenamed('round(avg(holders), 0)', 'robinhoodGoogHolders')

robinAaplDateDf.show()
robinMetaDateDf.show()
robinGoogDateDf.show()

# COMMAND ----------

robinAaplMeta = robinAaplDateDf.join(robinMetaDateDf, robinMetaDateDf.date == robinAaplDateDf.date).select([robinAaplDateDf.date, robinAaplDateDf.robinhoodAaplHolders, robinMetaDateDf.robinhoodMetaHolders])

robinAaplMetaGoog = robinAaplMeta.join(robinGoogDateDf, robinGoogDateDf.date == robinAaplMeta.date).select([robinAaplMeta.robinhoodAaplHolders, robinAaplMeta.robinhoodMetaHolders, robinGoogDateDf.robinhoodGoogHolders, robinGoogDateDf.date])

robinAaplMetaGoog.show()

# COMMAND ----------

aaplSigDf = aaplRdd.toDF(['date','aaplOpen','aaplHigh','aaplLow','aaplClose','aaplAdjClose','aaplVolume'])
metaSigDf = metaRdd.toDF(['date','metaOpen','metaHigh','metaLow','metaClose','metaAdjClose','metaVolume'])
googSigDf = googRdd.toDF(['date','googOpen','googHigh','googLow','googClose','googAdjClose','googVolume'])

aaplMetaGoogSigDf = aaplSigDf.join(metaSigDf, metaSigDf.date == aaplSigDf.date).join(googSigDf, googSigDf.date == aaplSigDf.date).select([aaplSigDf.date, 'aaplOpen', 'aaplHigh', 'aaplLow', 'aaplClose', 'aaplAdjClose', 'aaplVolume', 'metaOpen', 'metaHigh', 'metaLow', 'metaClose', 'metaAdjClose', 'metaVolume', 'googOpen', 'googHigh', 'googLow', 'googClose', 'googAdjClose', 'googVolume'])
aaplMetaGoogSigDf.show()

# COMMAND ----------

aaplMetaGoogRobin = aaplMetaGoogSigDf.alias('amg').join(robinAaplMetaGoog, robinAaplMetaGoog.date == aaplMetaGoogSigDf.date).select(['amg.*', 'robinhoodGoogHolders', 'robinhoodMetaHolders', 'robinhoodAaplHolders']).orderBy('date')
aaplMetaGoogRobin.show()

# COMMAND ----------

def getDateRange(list):
    return f"{list[0].date}:{list[len(list) - 1].date}"

aaplMetaGoogRobinList = aaplMetaGoogRobin.collect()
fig, ax = plt.subplots()
ax.plot([row.date for row in aaplMetaGoogRobinList], [row.robinhoodAaplHolders for row in aaplMetaGoogRobinList], color = 'blue', label = 'AAPL')
ax.plot([row.date for row in aaplMetaGoogRobinList], [row.robinhoodMetaHolders for row in aaplMetaGoogRobinList], color = 'green', label = 'META')
ax.plot([row.date for row in aaplMetaGoogRobinList], [row.robinhoodGoogHolders for row in aaplMetaGoogRobinList], color = 'red', label = 'GOOG')
ax.legend(loc = 'upper left')
plt.title('Robinhood')
plt.ylabel('Holders')
plt.xlabel(f"date ({getDateRange(aaplMetaGoogRobinList)})")
plt.tick_params(bottom=False, labelbottom=False)
plt.show()

fig, ax = plt.subplots()
ax.plot([row.date for row in aaplMetaGoogRobinList], [int(row.aaplVolume) for row in aaplMetaGoogRobinList], color = 'blue', label = 'AAPL')
ax.plot([row.date for row in aaplMetaGoogRobinList], [int(row.metaVolume) for row in aaplMetaGoogRobinList], color = 'green', label = 'META')
ax.plot([row.date for row in aaplMetaGoogRobinList], [int(row.googVolume) for row in aaplMetaGoogRobinList], color = 'red', label = 'GOOG')
ax.legend(loc = 'upper left')
plt.title('Exchange')
plt.ylabel('Volume')
plt.xlabel(f"date ({getDateRange(aaplMetaGoogRobinList)})")
plt.tick_params(bottom=False, labelbottom=False)
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Question 2:</strong> Get the average of prices of those 3 stocks for trend and comparison.

# COMMAND ----------

aaplAvg = aaplDf.select(['date', F.round((aaplDf.high + aaplDf.low)/2,6)]).withColumnRenamed('round(((high + low) / 2), 6)', 'avgPrice')
metaAvg = metaDf.select(['date', F.round((metaDf.high + metaDf.low)/2,6)]).withColumnRenamed('round(((high + low) / 2), 6)', 'avgPrice')
googAvg = googDf.select(['date', F.round((googDf.high + googDf.low)/2,6)]).withColumnRenamed('round(((high + low) / 2), 6)', 'avgPrice')

# COMMAND ----------

aaplAvgList = aaplAvg.collect()
plt.plot([row.date for row in aaplAvgList], [row.avgPrice for row in aaplAvgList])
plt.title('AAPL')
plt.ylabel('average price (USD)')
plt.xlabel(f"date ({getDateRange(aaplAvgList)})")
plt.tick_params(bottom=False, labelbottom=False)
plt.show()

# COMMAND ----------

metaAvgList = metaAvg.collect()
plt.plot([row.date for row in metaAvgList], [row.avgPrice for row in metaAvgList])
plt.title('META')
plt.ylabel('average price (USD)')
plt.xlabel(f"date ({getDateRange(metaAvgList)})")
plt.tick_params(bottom=False, labelbottom=False)
plt.show()

# COMMAND ----------

googAvgList = googAvg.collect()
plt.plot([row.date for row in googAvgList], [row.avgPrice for row in googAvgList])
plt.title('GOOG')
plt.ylabel('average price (USD)')
plt.xlabel(f"date ({getDateRange(googAvgList)})")
plt.tick_params(bottom=False, labelbottom=False)
plt.show()

# COMMAND ----------

fig, ax = plt.subplots()
ax.plot([row.avgPrice for row in aaplAvgList], color = 'green', label = 'AAPL')
ax.plot([row.avgPrice for row in metaAvgList], color = 'blue', label = 'META')
ax.plot([row.avgPrice for row in googAvgList], color = 'red', label = 'GOOG')
ax.legend(loc = 'upper left')
plt.title('AAPL vs META vs GOOG ')
plt.ylabel('average price (USD)')
plt.xlabel(f"date ({getDateRange(googAvgList)})")
plt.tick_params(bottom=False, labelbottom=False)
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Question 3:</strong> Compare the weekly transaction volume of the 3 stocks and see how that fluctuate based on the Covid19 case weekly total.

# COMMAND ----------

from dateutil.parser import parse
from datetime import datetime

covidRddFmt = covidRdd.map(lambda values: [parse(values[1]).strftime("%Y-%m-%d"),values[2]])
covidDfFmt = covidRddFmt.toDF(['date', 'weeklyCases']).orderBy('date')
covidDfFmt.show()

# COMMAND ----------

# cut down the dates on prices using covid df
volsDf = aaplMetaGoogSigDf.select(['date', 'aaplVolume', 'metaVolume', 'googVolume'])
volsDf = volsDf.withColumn("date", F.to_timestamp(F.col("date")))
volsDf = volsDf.select(F.date_format('date','yyyy-MM-dd').alias('date'), 'aaplVolume', 'metaVolume', 'googVolume')

volsCutDf = volsDf.filter(volsDf.date > covidDfFmt.first().date)
volsWeeklyDf = volsCutDf.withColumn("date",F.date_sub(F.next_day(F.col("date"),"wednesday"),7)).groupBy("date").agg(F.sum("aaplVolume").alias("aaplVolume"),F.sum("metaVolume").alias("metaVolume"),F.sum("googVolume").alias("googVolume")).orderBy("date")

covidVolsDf = volsWeeklyDf.alias("v").join(covidDfFmt, covidDfFmt.date == volsWeeklyDf.date).select(["v.*", covidDfFmt.weeklyCases]).orderBy("v.date")
covidVolsDf.show()

# COMMAND ----------

covidVolsList = covidVolsDf.collect()
plt.plot([row.date for row in covidVolsList], [int(row.weeklyCases) for row in covidVolsList])
plt.title('COVID Volume')
plt.ylabel('Cases')
plt.xlabel(f"date ({getDateRange(covidVolsList)})")
plt.show()

fig, ax = plt.subplots()
ax.plot([row.date for row in covidVolsList], [int(row.aaplVolume) for row in covidVolsList], color = 'blue', label = 'AAPL')
ax.plot([row.date for row in covidVolsList], [int(row.metaVolume) for row in covidVolsList], color = 'green', label = 'META')
ax.plot([row.date for row in covidVolsList], [int(row.googVolume) for row in covidVolsList], color = 'red', label = 'GOOG')
ax.legend(loc = 'upper left')
plt.title('Market')
plt.ylabel('Volume')
plt.xlabel(f"date ({getDateRange(aaplMetaGoogRobinList)})")
plt.tick_params(bottom=False, labelbottom=False)
plt.show()
