package edu.uchicago.cs.dbp.online.leopard.experiment.emailenron

import scala.collection.mutable.HashMap
import scala.io.Source
import edu.uchicago.cs.dbp.PartitionEvaluator
object Evaluate extends App {

  PartitionEvaluator.eval("leopard/emailenron/edge", "leopard/emailenron/p_leopard")
  PartitionEvaluator.eval("leopard/emailenron/edge", "leopard/emailenron/p_metis")

}