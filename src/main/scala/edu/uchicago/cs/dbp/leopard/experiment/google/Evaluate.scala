package edu.uchicago.cs.dbp.leopard.experiment.google

import scala.collection.mutable.HashMap
import scala.io.Source
import edu.uchicago.cs.dbp.leopard.experiment.common.Evaluator
object Evaluate extends App {

  Evaluator.eval("leopard/google/edge", "leopard/google/p_leopard")
  Evaluator.eval("leopard/google/edge", "leopard/google/p_metis")

}