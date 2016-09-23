package edu.uchicago.cs.dbp.online.opt.mip.experiment.stanford

import edu.uchicago.cs.dbp.PartitionEvaluator

object Evaluate extends App {

  PartitionEvaluator.eval("dataset/stanford/edge", "dataset/stanford/p_leopard")
  PartitionEvaluator.eval("dataset/stanford/edge", "dataset/stanford/p_mip")
  PartitionEvaluator.eval("dataset/stanford/edge", "dataset/stanford/p_metis")

}