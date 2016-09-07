package edu.uchicago.cs.dbp.online.leopard.experiment.astroph

import scala.collection.mutable.HashMap
import scala.io.Source
import edu.uchicago.cs.dbp.PartitionEvaluator
object Evaluate extends App {

  PartitionEvaluator.eval("leopard/astroph/edge", "leopard/astroph/p_leopard")
  PartitionEvaluator.eval("leopard/astroph/edge", "leopard/astroph/p_metis")

}