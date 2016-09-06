package edu.uchicago.cs.dbp.online.leopard.experiment.emailenron

import scala.collection.mutable.HashMap
import scala.io.Source
import edu.uchicago.cs.dbp.online.leopard.experiment.common.Evaluator
object Evaluate extends App {

  Evaluator.eval("leopard/emailenron/edge", "leopard/emailenron/p_leopard")
  Evaluator.eval("leopard/emailenron/edge", "leopard/emailenron/p_metis")

}