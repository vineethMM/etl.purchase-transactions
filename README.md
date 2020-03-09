Purchase Transactions
======================

## Problem statement
We have a transaction data flowing in from a source. The types of transactions would be either a purchase, a refund or a void transaction. Both refund transactions and the void transactions would have a parent transaction. Considering that we have millions of transactions flowing in from the source; we need to find the total refunds per customer each month.
When finding total refunds,
 1. Any transaction which has any of its child transaction as a void transaction should be ignored.
 
 2. Any refund transaction for which the parent transaction happened in the same month also should be ignored.

Implement the derivation of the total refunds monthly feature


## Solution description
This solution uses the Hadoop eco-system with following following components

1. HDFS 
2. YARN
3. 

### Tech stack

![Tech stack](src/main/docs/tech_stack.png)

source -> edge node -> HDFS -> Hive -> Spark 

Other options to consider are

Kafka/ Sqoop -> HDFS/S3 -> YARN(EMR)/kubernetes -> Spark 

