package edu.uchicago.cs.dbp

import edu.uchicago.cs.dbp.model.Edge

trait Partitioner {
  def add(e: Edge);
}