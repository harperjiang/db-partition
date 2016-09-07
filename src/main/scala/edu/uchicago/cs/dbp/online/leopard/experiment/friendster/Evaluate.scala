package edu.uchicago.cs.dbp.online.leopard.experiment.friendster

import scala.collection.mutable.HashMap
import scala.io.Source
import edu.uchicago.cs.dbp.PartitionEvaluator
object Evaluate extends App {

  PartitionEvaluator.eval("leopard/friendster/edge", "leopard/friendster/p_leopard")
  PartitionEvaluator.eval("leopard/friendster/edge", "leopard/friendster/p_metis")

}