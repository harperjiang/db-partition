package edu.uchicago.cs.dbp.online.leopard.experiment.berkstan

import scala.collection.mutable.HashMap
import scala.io.Source
import edu.uchicago.cs.dbp.PartitionEvaluator
object Evaluate extends App {

  PartitionEvaluator.eval("leopard/berkstan/edge", "leopard/berkstan/p_leopard")
  PartitionEvaluator.eval("leopard/berkstan/edge", "leopard/berkstan/p_metis")

}