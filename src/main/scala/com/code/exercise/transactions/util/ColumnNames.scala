package com.code.exercise.transactions.util

object ColumnNames {

  val TRAN_ID      = "transaction_id"
  val PRNT_TRAN_ID = "parent_transaction_id"
  val TRAN_D       = "transaction_date"
  val TRAN_A       = "transaction_amount"
  val ACCT_N       = "account_number"

  val CHLD_TRAN_ID = "child_transaction_id"
  val CHLD_TRAN_D  = "child_transaction_date"
  val CHLD_TRAN_A  = "child_transaction_amount"

  val CUST_ID       = "customer_id"
  val CUST_NAME     = "customer_name"
  val NUM_REFD_TRAN = "number_of_refund_transactions"
}
