package edu.uchicago.cs.dbp.online.leopard.experiment.google

import scala.collection.mutable.HashMap
import scala.io.Source
import edu.uchicago.cs.dbp.PartitionEvaluator
object Evaluate extends App {

  PartitionEvaluator.eval("leopard/google/edge", "leopard/google/p_leopard")
  PartitionEvaluator.eval("leopard/google/edge", "leopard/google/p_metis")

}