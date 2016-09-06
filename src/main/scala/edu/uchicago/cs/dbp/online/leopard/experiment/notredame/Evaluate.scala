package edu.uchicago.cs.dbp.online.leopard.experiment.notredame

import scala.collection.mutable.HashMap
import scala.io.Source
import edu.uchicago.cs.dbp.online.leopard.experiment.common.Evaluator
object Evaluate extends App {

  Evaluator.eval("leopard/notredame/edge", "leopard/notredame/p_leopard")
  Evaluator.eval("leopard/notredame/edge", "leopard/notredame/p_metis")

}