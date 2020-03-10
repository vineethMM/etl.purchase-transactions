package com.code.exercise.transactions

import com.code.exercise.transactions.transform.CanProcessRefund
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.lit

import com.code.exercise.transactions.util.Constants._

object CustomerRefundTransactions extends CanProcessRefund {

  def process(ss: SparkSession, outputTableName: String, yearMonth: String) {
    // Validations can be carried out to check if the tables exists
    // and exist with proper error message
    val purchaseTransactions  = ss.table(s"${TRAN_DB}.${TRAN_TABL}")
    val accountsWithCustomers = ss.table(s"${TRAN_DB}.${CUST_ACCT_TABL}")

    val refundTransactions = getRefundTransactions(purchaseTransactions)

    refundTransactionsPerCustomer(refundTransactions, accountsWithCustomers)
      .withColumn("year-month", lit(yearMonth))
      .write
      .mode(SaveMode.Overwrite)                         // Only applied to the partition
      .partitionBy("year-month")             // partition based on ETL year-month
      .saveAsTable(outputTableName)
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Customer-Refund-Transactions")
      .enableHiveSupport()
      .getOrCreate()

    spark.sqlContext.setConf("hive.exec.dynamic.partition", "true")
    spark.sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")

    // Could you command line argument parsing libraries here
    CustomerRefundTransactions.process(spark, args(0), args(1))
  }
}
