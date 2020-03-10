package com.code.exercise.transactions.transform

import com.code.exercise.test.support.SparkSpec
import org.apache.spark.sql.Row

class CanProcessRefundSpec  extends SparkSpec with CanProcessRefund {
  // Note: these test dot not have full coverage

  "CanProcessRefund.getRefundTransactions" should "only return refund transactions" in {
    val spark = session
    import spark.implicits._

    val inputDF = List(
      ("Tran1", null   , "purchase", 10.0, "acc1", "2020-03-03"),
      ("Tran2", "Tran1", "refund"  , 10.0, "acc4", "2020-04-03"),
      ("Tran3", null   , "purchase",  1.0, "acc2", "2020-03-03")
    ).toDF("transaction_id", "parent_transaction_id", "transaction_type","transaction_amount", "account_number", "transaction_date")

    val expectedOutput = Array(Row("Tran1", 10.0, "acc1", "2020-03-03", "Tran2", 10.0, "2020-04-03", "refund"))
    val actualOutputDf = getRefundTransactions(inputDF)

    val actualOutput = actualOutputDf.collect()

    actualOutput should equal( expectedOutput)
  }

  "CanProcessRefund.refundTransactionsPerCustomer" should "only return refund transactions" in {
    val spark = session
    import spark.implicits._

    val refundTransactions = List(
      ("Tran1", "refund", 10.0, "acc1", "2020-03-03")
    ).toDF("transaction_id", "transaction_type","transaction_amount", "account_number", "transaction_date")

    // val CUST_ID       = "customer_id"
    // val CUST_NAME     = "customer_name"
    val customerAccount = List(
      ("cust_1", "abcd", "acc1"),
      ("cust_2", "qwer", "acc2")
    ).toDF("customer_id", "customer_name", "account_number")

    val expectedOutput = Array(Row("cust_1", 1))
    val actualOutputDf = refundTransactionsPerCustomer(refundTransactions, customerAccount)

    val actualOutput = actualOutputDf.collect()

    actualOutput should equal( expectedOutput)
  }
}
