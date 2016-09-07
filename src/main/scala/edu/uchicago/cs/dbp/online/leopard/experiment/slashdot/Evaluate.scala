package edu.uchicago.cs.dbp.online.leopard.experiment.slashdot

import scala.collection.mutable.HashMap
import scala.io.Source
import edu.uchicago.cs.dbp.PartitionEvaluator
object Evaluate extends App {

  PartitionEvaluator.eval("leopard/slashdot/edge", "leopard/slashdot/p_leopard")
  PartitionEvaluator.eval("leopard/slashdot/edge", "leopard/slashdot/p_metis")

}