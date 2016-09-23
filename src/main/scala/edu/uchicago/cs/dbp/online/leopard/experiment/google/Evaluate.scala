package edu.uchicago.cs.dbp.online.leopard.experiment.google

import scala.collection.mutable.HashMap
import scala.io.Source
import edu.uchicago.cs.dbp.PartitionEvaluator
object Evaluate extends App {

  PartitionEvaluator.eval("dataset/google/edge", "dataset/google/p_leopard")
  PartitionEvaluator.eval("dataset/google/edge", "dataset/google/p_metis")

}