package edu.uchicago.cs.dbp.online.opt.mip.experiment.astroph

import edu.uchicago.cs.dbp.PartitionEvaluator

object Evaluate extends App {

  PartitionEvaluator.eval("dataset/astroph/edge", "dataset/astroph/p_leopard")
  PartitionEvaluator.eval("dataset/astroph/edge", "dataset/astroph/p_mip")
  PartitionEvaluator.eval("dataset/astroph/edge", "dataset/astroph/p_metis")

}