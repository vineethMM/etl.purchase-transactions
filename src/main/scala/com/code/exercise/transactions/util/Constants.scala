package com.code.exercise.transactions.util

object Constants {

  val TRAN_ID      = "transaction_id"
  val PRNT_TRAN_ID = "parent_transaction_id"
  val TRAN_D       = "transaction_date"
  val TRAN_A       = "transaction_amount"
  val ACCT_N       = "account_number"
  val TRAN_TYPE    = "transaction_type"

  val CHLD_TRAN_ID = "child_transaction_id"
  val CHLD_TRAN_D  = "child_transaction_date"
  val CHLD_TRAN_A  = "child_transaction_amount"
  val CHLD_ACCT_N  = "child_account_number"

  val CUST_ID       = "customer_id"
  val CUST_NAME     = "customer_name"
  val NUM_REFD_TRAN = "number_of_refund_transactions"

  val TRAN_DB        = "domain"
  val TRAN_TABL      = "purchase_transactions"
  val CUST_ACCT_TABL = "customer_account_table"

  val TRAN_TYPE_PRCH = "purchase"
  val TRAN_TYPE_RFND = "refund"
  val TRAN_TYPE_VOID = "void"
}
