# cassandra
Nosql databases wont support Joins. Also they are not designed for joining tables.
But if you need to do some join of Nosql tables, (may be due to your bad data modeling), you can acheive that by doing the join in 
Spark.
Here I am giving a sample code to join two datasets from cassandra using spark-sql
(Join 2 cassandra tables using spark-scala)
My cassandra is installed locally.
Two temp views formed in Spark and they are joined in memory and the result dataset is stored back in a cassandra table which is dynamically created by code.

You may face issue while dumping data into cassandra saying required 2 nodes but only one is alive.
This is because you might have created your cassandra keyspace with a replication factor>1.
So, make the keyspace with only 1 replication factor as we are working locally.
