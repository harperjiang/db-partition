package edu.uchicago.cs.dbp.online.leopard.experiment.orkut

import scala.collection.mutable.HashMap
import scala.io.Source
import edu.uchicago.cs.dbp.PartitionEvaluator
object Evaluate extends App {

  PartitionEvaluator.eval("leopard/orkut/edge", "leopard/orkut/p_leopard")
  PartitionEvaluator.eval("leopard/orkut/edge", "leopard/orkut/p_metis")

}