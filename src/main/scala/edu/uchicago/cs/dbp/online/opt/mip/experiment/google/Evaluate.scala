package edu.uchicago.cs.dbp.online.opt.mip.experiment.google

import edu.uchicago.cs.dbp.PartitionEvaluator

object Evaluate extends App {

  PartitionEvaluator.eval("dataset/google/edge", "dataset/google/p_leopard")
  PartitionEvaluator.eval("dataset/google/edge", "dataset/google/p_mip")
  PartitionEvaluator.eval("dataset/google/edge", "dataset/google/p_metis")

}