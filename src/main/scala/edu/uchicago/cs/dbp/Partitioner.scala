package edu.uchicago.cs.dbp

import edu.uchicago.cs.dbp.model.Edge
import edu.uchicago.cs.dbp.model.Partition

trait Partitioner {
  def add(e: Edge);
  def partitions: Iterable[Partition];
}