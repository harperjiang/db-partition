package edu.uchicago.cs.dbp.online.leopard.experiment.slashdot

import scala.collection.mutable.HashMap
import scala.io.Source
import edu.uchicago.cs.dbp.online.leopard.experiment.common.Evaluator
object Evaluate extends App {

  Evaluator.eval("leopard/slashdot/edge", "leopard/slashdot/p_leopard")
  Evaluator.eval("leopard/slashdot/edge", "leopard/slashdot/p_metis")

}