package edu.uchicago.cs.dbp.online.leopard.experiment.livejournal

import scala.collection.mutable.HashMap
import scala.io.Source
import edu.uchicago.cs.dbp.PartitionEvaluator
object Evaluate extends App {

  PartitionEvaluator.eval("leopard/livejournal/edge", "leopard/livejournal/p_leopard")
  PartitionEvaluator.eval("leopard/livejournal/edge", "leopard/livejournal/p_metis")

}