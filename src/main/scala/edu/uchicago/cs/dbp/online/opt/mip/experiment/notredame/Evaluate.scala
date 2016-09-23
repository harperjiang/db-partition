package edu.uchicago.cs.dbp.online.opt.mip.experiment.notredame

import edu.uchicago.cs.dbp.PartitionEvaluator

object Evaluate extends App {

  PartitionEvaluator.eval("dataset/notredame/edge", "dataset/notredame/p_leopard")
  PartitionEvaluator.eval("dataset/notredame/edge", "dataset/notredame/p_mip")
  PartitionEvaluator.eval("dataset/notredame/edge", "dataset/notredame/p_metis")

}