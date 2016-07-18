package edu.uchicago.cs.dbp.leopard.experiment.orkut

import scala.collection.mutable.HashMap
import scala.io.Source
import edu.uchicago.cs.dbp.leopard.experiment.common.Evaluator
object Evaluate extends App {

  Evaluator.eval("leopard/orkut/edge", "leopard/orkut/p_leopard")
  Evaluator.eval("leopard/orkut/edge", "leopard/orkut/p_metis")

}