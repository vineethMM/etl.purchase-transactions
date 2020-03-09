package com.code.exercise.transactions

import java.io.File

import com.code.exercise.transactions.transform.CanProcessRefund
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, lit}

import com.code.exercise.transactions.util.Constants._


object CustomerRefundTransactions extends CanProcessRefund {

  def process(ss: SparkSession, outputTableName: String, outputLocation: String, yearMonth: String) {
    val purchaseTransactions  = ss.table(s"${TRAN_DB}.${TRAN_TABL}")
    val accountsWithCustomers = ss.table(s"${TRAN_DB}.${CUST_ACCT_TABL}")

    val refundTransactions = getRefundTransactions(purchaseTransactions)

    refundTransactionsPerCustomer(refundTransactions, accountsWithCustomers)
      .withColumn("year-month", lit(yearMonth))
      .write
      .format("parquet")                       // Write data in parquet format
      .option("path", outputLocation)                   // specify an external location
      .mode(SaveMode.Overwrite)                         // Only applied to the partition
      .partitionBy("year-month")             // partition based on ETL year-month
      .saveAsTable(outputTableName)
  }

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Customer- Refund-Transactions")
      .enableHiveSupport()
      .getOrCreate()

    spark.sqlContext.setConf("hive.exec.dynamic.partition", "true")
    spark.sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")

    CustomerRefundTransactions.process(spark, args(0), args(1), args(2))
  }
}
