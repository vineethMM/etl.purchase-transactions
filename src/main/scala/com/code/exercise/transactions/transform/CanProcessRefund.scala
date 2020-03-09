package com.code.exercise.transactions.transform

import com.code.exercise.transactions.util.Constants._
import org.apache.spark.sql.functions.{col, count, month}
import org.apache.spark.sql.{DataFrame, SparkSession}

trait CanProcessRefund {

  /**
   * @param allTransactions
   *                       The input data frame should have following columns. (No compile time type safety as we are using DataFrame API)
   *                       1. transaction_id: String         -  Unique identifier for a transaction
   *                       2. parent_transaction_id: String  -  Reference the parent transaction
   *                       3. account_number: String         -  Debit account number
   *                       4. transaction_amount: Decimal(16, 2) -  Transaction amount
   *                       5. transaction_date: Timestamp    -  Timestamp at which the transaction occurred
   *
   * @return Transactions that has refunds along with refund information
   */
  def getRefundTransactions(allTransactions: DataFrame): DataFrame = {
    val cachedTransactions =  allTransactions.cache()

    // transaction whose parent_transaction_id is null is considered as parent transaction
    val parentTransactions =
      cachedTransactions
        .filter(col(PRNT_TRAN_ID).isNull)
        .drop(PRNT_TRAN_ID) // drop it, as it will be always null here

    // finds all child transactions
    val childTransactions  = cachedTransactions
      .filter(col(PRNT_TRAN_ID).isNotNull)
      .select(
        col(TRAN_ID) alias CHLD_TRAN_ID,
        col(PRNT_TRAN_ID),
        col(TRAN_A) alias CHLD_TRAN_A,
        col(TRAN_D) alias CHLD_TRAN_D
      )

     // Join parent and child transactions based on parent_transaction_id
     // and filter out transaction with void child transactions (i.e transaction_amount is null) and
     // refund transaction on happened on same month.
      parentTransactions
        .join(childTransactions, col(TRAN_ID) === col(PRNT_TRAN_ID))
        .drop(PRNT_TRAN_ID) // not required as same information is there in `transaction_id`
        .filter(col("child_transaction_amount").isNotNull) // Ignore transactions which has void child transactions
        .filter(month(col(TRAN_D)) =!= month(col(CHLD_TRAN_D))) // Ignore refund transactions happened on same month
  }

  /**
   * @param refundTransactions - refund transactions with at lease following columns in it
   *                           1. customerIs
   *
   * @param acctWithCustomer   - relationship between accounts and customer, at lease with following columns in it
   *                           1. customer_id   : Unique identifier of a customer
   *                           2. account_id    : Unique identifier of an account
   *                           3. customer_name : Name of the customer
   *
   * @return customer_id and the total number of refund transactions per customer
   */
  def refundTransactionsPerCustomer(refundTransactions: DataFrame, acctWithCustomer: DataFrame): DataFrame = {
    refundTransactions
      .join(acctWithCustomer.select(col(CUST_ID), col(CUST_NAME), col(ACCT_N)), col(ACCT_N) === col(ACCT_N))
      .groupBy(col(CUST_ID))
      .agg(count(CUST_ID).alias(NUM_REFD_TRAN))
  }
}
