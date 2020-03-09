package com.code.exercise.transactions

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._


trait CanProcessRefund {

  /*
   * The input data frame should have following columns. (No compile time type safety as we are using DataFrame API)
   *
   * 1. transaction_id: String         -  Unique identifier for a transaction
   * 2. parent_transaction_id: String  -  Reference the parent transaction
   * 3. is_dummy: Boolean              -  Indicates whether this is void transaction or not
   * 4. account_number: String         -  Debit account number
   * 5. transaction_amount: Decimal(16, 2) -  Transaction amount
   * 5. transaction_date: Timestamp    -  Timestamp at which the transaction occurred
   */

  def getRefundTransactions(allTransactions: DataFrame)(ss: SparkSession): DataFrame = {
    import ss.implicits._

    val cachedTransactions =  allTransactions.cache()

    // transaction whose parent_transaction_id is null is considered as parent transaction
    val parentTransactions =
      cachedTransactions
        .filter($"parent_transaction_id".isNull)
        .drop("parent_transaction_id") // drop it, as it will be always null here

    // finds all child transactions
    val childTransactions  = cachedTransactions
      .filter($"parent_transaction_id".isNotNull)
      .select(
        $"transaction_id" alias "child_transaction_id",
        $"parent_transaction_id",
        $"is_dummy" alias "child_is_dummy",
        $"transaction_date" alias "child_transaction_date"
      )

    // Join parent and child transactions based on parent_transaction_id
    // and filter out transaction with void child transactions and
    // refund transaction on happened on same month.
      parentTransactions
        .join(childTransactions, $"transaction_id" === $"parent_transaction_id")
        .drop("parent_transaction_id") // not required as same information is there in `transaction_id`
        .filter($"child_is_dummy" === false) // Ignore transactions which has void child transactions
        .filter(month($"transaction_date") =!= month($"child_transaction_date")) // Ignore refund transactions happened on same month
  }

}
