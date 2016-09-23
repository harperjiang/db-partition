package edu.uchicago.cs.dbp.online.opt.mip.experiment.livejournal

import edu.uchicago.cs.dbp.PartitionEvaluator

object Evaluate extends App {

  PartitionEvaluator.eval("dataset/livejournal/edge", "dataset/livejournal/p_leopard")
  PartitionEvaluator.eval("dataset/livejournal/edge", "dataset/livejournal/p_mip")
  PartitionEvaluator.eval("dataset/livejournal/edge", "dataset/livejournal/p_metis")

}