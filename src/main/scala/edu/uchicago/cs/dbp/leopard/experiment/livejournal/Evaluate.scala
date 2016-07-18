package edu.uchicago.cs.dbp.leopard.experiment.livejournal

import scala.collection.mutable.HashMap
import scala.io.Source
import edu.uchicago.cs.dbp.leopard.experiment.common.Evaluator
object Evaluate extends App {

  Evaluator.eval("leopard/livejournal/edge", "leopard/livejournal/p_leopard")
  Evaluator.eval("leopard/livejournal/edge", "leopard/livejournal/p_metis")

}