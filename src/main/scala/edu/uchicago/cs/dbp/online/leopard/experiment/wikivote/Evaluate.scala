package edu.uchicago.cs.dbp.online.leopard.experiment.wikivote

import scala.collection.mutable.HashMap
import scala.io.Source
import edu.uchicago.cs.dbp.online.leopard.experiment.common.Evaluator
object Evaluate extends App {

  Evaluator.eval("leopard/wikivote/edge", "leopard/wikivote/p_leopard")
  Evaluator.eval("leopard/wikivote/edge", "leopard/wikivote/p_metis")

  
}