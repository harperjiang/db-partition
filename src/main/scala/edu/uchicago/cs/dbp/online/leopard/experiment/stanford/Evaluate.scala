package edu.uchicago.cs.dbp.online.leopard.experiment.stanford

import scala.collection.mutable.HashMap
import scala.io.Source
import edu.uchicago.cs.dbp.online.leopard.experiment.common.Evaluator
object Evaluate extends App {

  Evaluator.eval("leopard/stanford/edge", "leopard/stanford/p_leopard")
  Evaluator.eval("leopard/stanford/edge", "leopard/stanford/p_metis")

}