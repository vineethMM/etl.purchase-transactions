package com.code.exercise.transactions

import com.code.exercise.test.support.SparkSpec
import org.apache.spark.sql.Row

class CanProcessRefundSpec  extends SparkSpec with CanProcessRefund {

  // Not a great test case, intended for demo purpose
  "CanProcessRefund.getRefundTransactions" should "only return refund transactions" in {
    val spark = session
    import spark.implicits._

    val inputDF = List(
      // transaction_id, parent_transaction_id, is_dummy, account_number, transaction_date
      ("Tran1", null, 10.0, "acc1", "2020-03-03"),
      ("Tran2", "Tran1", 10.0, "acc4", "2020-04-03"),
      ("Tran3", null, 1.0, "acc2", "2020-03-03")
    ).toDF("transaction_id", "parent_transaction_id", "transaction_amount", "account_number", "transaction_date")

   val expectedOutput = Array(Row("Tran1", 10.0, "acc1", "2020-03-03", "Tran2", 10.0, "2020-04-03"))
   val actualOutputDf = getRefundTransactions(inputDF)(session)

   val actualOutput = actualOutputDf.collect()

   actualOutput should equal( expectedOutput)
  }
}
