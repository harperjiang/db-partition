package edu.uchicago.cs.dbp.online.opt.mip.experiment.emailenron

import edu.uchicago.cs.dbp.PartitionEvaluator

object Evaluate extends App {

  PartitionEvaluator.eval("dataset/emailenron/edge", "dataset/emailenron/p_leopard")
  PartitionEvaluator.eval("dataset/emailenron/edge", "dataset/emailenron/p_mip")
  PartitionEvaluator.eval("dataset/emailenron/edge", "dataset/emailenron/p_metis")

}