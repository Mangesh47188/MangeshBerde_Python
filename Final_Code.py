from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import HiveContext
from pyspark.sql.types import *
from pyspark.sql.window import Window
import pyspark.sql.functions as func

sqlContext1 = SQLContext(sc)

sqlContext = HiveContext(sc)

import sys


//sandbox.hortonwork.com:8020 hdfs://sandbox.hortonwork.com:8020/


def main():
    try:

			df=sqlContext1.read.json('transaction.json').select("Instrument","TransactionId","TransactionQuantity","TransactionType")
			df.registerTempTable("tmp_Transaction")
			sqlContext1.sql("select * from tmp_Transaction").show
			rd_start=sc.textFile("startDay.txt")
			header = rd_start.first()

			rd_start = rd_start.filter(lambda line: line != header)
			temp_var = rd_start.map(lambda k: k.split(","))


			start_df=temp_var.toDF(header.split(","))
			start_df.show()


			start_df.registerTempTable("Start_day")
			df_stock_stage=sqlContext1.sql("select s.*,t.TransactionId,t.TransactionQuantity,t.TransactionType from Start_day s inner join tmp_Transaction t on 

			t.Instrument=s.Instrument")
			df_stock_stage.registerTempTable("tmp_stage_Transaction")

			df_final=sqlContext1.sql("""select Instrument,AccountType,Account,TransactionId,Quantity as qty,TransactionQuantity,TransactionType , (case when 
			TransactionType='B' and AccountType='E' then Quantity+TransactionQuantity when TransactionType='B' and AccountType='I' then Quantity-TransactionQuantity 
			when TransactionType='S' and AccountType='E' then Quantity-TransactionQuantity when TransactionType='S' and AccountType='I' then Quantity
			+TransactionQuantity end ) as sale_qty from tmp_stage_Transaction order by Instrument,TransactionId """) 

			from pyspark.sql.window import Window
			from pyspark.sql import HiveContext

			assert isinstance(sqlContext, HiveContext)
			w =  Window.partitionBy(df_final.Instrument,df_final.Account).orderBy(df_final.Instrument,df_final.TransactionId)
			
			df_final.select(lag(df_Total.Quantity).over(w).alias("lag"))

			df_Total=df_final.select ("Instrument","AccountType","Account","TransactionId","qty","TransactionQuantity","TransactionType","qty"+lag("qty",1).over

			(Window.partitionBy("Instrument").orderby("TransactionId").alias("Total") )

			df_Total.registerTempTable("Final_table")
			
		  except Exception, err:
            print 'Caught an exception'
            return 1
    finally:
        print 'In finally block for cleanup'