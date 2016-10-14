#db-partition version 0.0.1

db-partition is a research project for database partitioning.

Partitioning is crucial for all kinds of distributed systems who need to hold stable data in memory. An efficient partitioning method can maximize local computation and thus minimize communication between work nodes.

Our current study maps data set to be partitioned to a graph, and apply various graph-partitioning methods to it. 

## Online Partitioner

Online partitioner employs a one-pass method which scan each edge in the graph at most once. It is ideal for large data sets for which it is impossible to hold all data in memory. 

### Leopard Partitioner

`LeopardPartitioner` provides an implementation to the method described [here](http://www.vldb.org/pvldb/vol9/p540-huang.pdf)

### MIPPartitioner

`MIPPartitioner` is an implementation using relaxed linear program to solve the problem.

### MLayerPartitioner