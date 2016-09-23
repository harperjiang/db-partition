package edu.uchicago.cs.dbp.online.opt.mip.experiment.berkstan

import edu.uchicago.cs.dbp.PartitionEvaluator

object Evaluate extends App {

  PartitionEvaluator.eval("dataset/berkstan/edge", "dataset/berkstan/p_leopard")
  PartitionEvaluator.eval("dataset/berkstan/edge", "dataset/berkstan/p_mip")
  PartitionEvaluator.eval("dataset/berkstan/edge", "dataset/berkstan/p_metis")

}