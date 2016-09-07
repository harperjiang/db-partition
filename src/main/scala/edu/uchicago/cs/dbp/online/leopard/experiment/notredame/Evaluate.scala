package edu.uchicago.cs.dbp.online.leopard.experiment.notredame

import scala.collection.mutable.HashMap
import scala.io.Source
import edu.uchicago.cs.dbp.PartitionEvaluator
object Evaluate extends App {

  PartitionEvaluator.eval("leopard/notredame/edge", "leopard/notredame/p_leopard")
  PartitionEvaluator.eval("leopard/notredame/edge", "leopard/notredame/p_metis")

}