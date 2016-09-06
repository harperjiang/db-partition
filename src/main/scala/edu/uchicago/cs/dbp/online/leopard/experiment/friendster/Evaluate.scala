package edu.uchicago.cs.dbp.online.leopard.experiment.friendster

import scala.collection.mutable.HashMap
import scala.io.Source
import edu.uchicago.cs.dbp.online.leopard.experiment.common.Evaluator
object Evaluate extends App {

  Evaluator.eval("leopard/friendster/edge", "leopard/friendster/p_leopard")
  Evaluator.eval("leopard/friendster/edge", "leopard/friendster/p_metis")

}