package edu.uchicago.cs.dbp.online.leopard.experiment.berkstan

import scala.collection.mutable.HashMap
import scala.io.Source
import edu.uchicago.cs.dbp.online.leopard.experiment.common.Evaluator
object Evaluate extends App {

  Evaluator.eval("leopard/berkstan/edge", "leopard/berkstan/p_leopard")
  Evaluator.eval("leopard/berkstan/edge", "leopard/berkstan/p_metis")

}