package edu.uchicago.cs.dbp

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Buffer
import edu.uchicago.cs.dbp.model.Partition

class AbstractPartitioner(numPartition: Int) {
  var partitions: Buffer[Partition] = new ArrayBuffer[Partition];

  for (i <- 0 until numPartition) {
    partitions += new Partition(i);
  }
}