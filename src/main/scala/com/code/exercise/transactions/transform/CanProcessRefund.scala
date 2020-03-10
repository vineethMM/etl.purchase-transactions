package com.code.exercise.transactions.transform

import org.apache.spark.sql.functions.{col, count, month}
import org.apache.spark.sql.DataFrame

import com.code.exercise.transactions.util.Constants._

trait CanProcessRefund {
  /**
   * @param allTransactions
   *                       The input data frame should have following columns. (No compile time type safety as we are using DataFrame API)
   *                       1. transaction_id: String         -  Unique identifier for a transaction
   *                       2. parent_transaction_id: String  -  Reference the parent transaction
   *                       3. transaction_type               -  type of transaction (purchase, refund or void)
   *                       4. account_number: String         -  Debit account number
   *                       5. transaction_amount: Decimal(16, 2) -  Transaction amount
   *                       6. transaction_date: Timestamp    -  Timestamp at which the transaction occurred
   *
   * @return Transactions that has refunds along with refund information
   */
  def getRefundTransactions(allTransactions: DataFrame): DataFrame = {
    val cachedTransactions =  allTransactions.cache()

    // transaction whose parent_transaction_id is null is considered as parent transaction
    val purchaseTransactions =
      cachedTransactions
        .filter(s"$TRAN_TYPE = '$TRAN_TYPE_PRCH'")
        .drop(TRAN_TYPE)    // drop it, as it won't be used again
        .drop(PRNT_TRAN_ID) // Assumption is purchase transaction won't have a parent_transaction_id

    // find all refund transactions
    val refundTransactions  =
      cachedTransactions
        .select(
          col(TRAN_ID) alias CHLD_TRAN_ID,
          col(PRNT_TRAN_ID),
          col(TRAN_A) alias CHLD_TRAN_A,
          col(TRAN_D) alias CHLD_TRAN_D,
          col(TRAN_TYPE)
        )
        .filter( s"$TRAN_TYPE = '$TRAN_TYPE_RFND'")

    // find all void transactions
    val voidTransactions  =
      cachedTransactions
        .select(
          col(TRAN_ID) alias "void_transaction_id",
          col(PRNT_TRAN_ID)
        )
        .filter( s"$TRAN_TYPE = '$TRAN_TYPE_VOID'")


    // Inner join parent and child transactions based on parent_transaction_id to find i
    // and filter out transaction with void child transactions (i.e transaction_amount is null) and
    // refund transaction on happened on same month.
    val purchasesWithRefund =
      purchaseTransactions
        .join(refundTransactions, col(TRAN_ID) === col(PRNT_TRAN_ID))
        .drop(PRNT_TRAN_ID) // not required as same information is there in `transaction_id`
        .filter(month(col(TRAN_D)) =!= month(col(CHLD_TRAN_D))) // Ignore refund transactions happened on same month

    // Finally remove all transactions which have a void child
    // Use left semi join for the same.
    purchasesWithRefund
      .join(voidTransactions, col(TRAN_ID) ===  col(PRNT_TRAN_ID), "left_anti")
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
    val acctWithCustomerProjection =
      acctWithCustomer
        .select(
          col(CUST_ID),
          col(CUST_NAME),
          col(ACCT_N) alias CHLD_ACCT_N
        )

    // Join refund transactions with customer_account relationship table
    // and roll up to find number of refund transactions in a month.
    refundTransactions
      .join(acctWithCustomerProjection, col(ACCT_N) === col(CHLD_ACCT_N))
      .groupBy(col(CUST_ID))
      .agg(count(CUST_ID).alias(NUM_REFD_TRAN))
  }
}
