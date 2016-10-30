#db-partition version 0.0.1

db-partition is a research project for database partitioning.

Partitioning is crucial for all kinds of distributed systems who need to hold stable data in memory. An efficient partitioning method can maximize local computation and thus minimize communication between work nodes.

Our current study maps data set to be partitioned to a graph, and apply various graph-partitioning methods to it. 

## Online Partitioner

Online partitioner employs a one-pass method which scan each edge in the graph at most once. It is ideal for large data sets for which it is impossible to hold all data in memory. 

### Leopard Partitioner

`LeopardPartitioner` provides an implementation to the method described [here](http://www.vldb.org/pvldb/vol9/p540-huang.pdf)

### Mixed Integer Partitioner

`MIPPartitioner` employs an algorithm using relaxed linear program to do online partitioning.

### MLayerPartitioner

`MLayerPartitioner` targets at solving the problem that other online partitioners don't work well on power-law graphs. Power-law graphs exhibits the following properties: very few nodes have extremely high degrees and most nodes have low degrees. `MLayerPartitioner` will group vertices into hyper vertices and each hyper vertex will be assigned to a partition, together with all vertices under it.