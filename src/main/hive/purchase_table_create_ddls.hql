--  DDL for purchase transactions
CREATE EXTERNAL TABLE domain.purchase_transactions(
  transaction_id string,
  parent_transaction_id string,
  transaction_type string,
  account_number string,
  transaction_amount Decimal(19, 2),
  transaction_date Timestamp
)
PARTITIONED BY (year string, month string)
LOCATION '/home/HDFS/domain.db/purchase_transactions'
TBLPROPERTIES('serialization.null.format'='')
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|';

-- DDL account-customer relationship
CREATE EXTERNAL TABLE domain.customer_account_table(
  customer_id string,
  customer_name string,
  account_number string
) LOCATION '/home/HDFS/domain.db/customer_account_table'
TBLPROPERTIES('serialization.null.format'='')
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|';

