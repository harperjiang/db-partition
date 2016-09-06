package edu.uchicago.cs.dbp.online.leopard.experiment.astroph

import scala.collection.mutable.HashMap
import scala.io.Source
import edu.uchicago.cs.dbp.online.leopard.experiment.common.Evaluator
object Evaluate extends App {

  Evaluator.eval("leopard/astroph/edge", "leopard/astroph/p_leopard")
  Evaluator.eval("leopard/astroph/edge", "leopard/astroph/p_metis")

}