package edu.uchicago.cs.dbp.online.leopard.experiment.stanford

import scala.collection.mutable.HashMap
import scala.io.Source
import edu.uchicago.cs.dbp.PartitionEvaluator
object Evaluate extends App {

  PartitionEvaluator.eval("leopard/stanford/edge", "leopard/stanford/p_leopard")
  PartitionEvaluator.eval("leopard/stanford/edge", "leopard/stanford/p_metis")

}