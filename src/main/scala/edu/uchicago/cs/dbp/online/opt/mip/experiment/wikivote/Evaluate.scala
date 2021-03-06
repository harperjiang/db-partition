package edu.uchicago.cs.dbp.online.opt.mip.experiment.wikivote

import edu.uchicago.cs.dbp.PartitionEvaluator

object Evaluate extends App {

  PartitionEvaluator.eval("dataset/wikivote/edge", "dataset/wikivote/p_leopard")
  PartitionEvaluator.eval("dataset/wikivote/edge", "dataset/wikivote/p_mip")
  PartitionEvaluator.eval("dataset/wikivote/edge", "dataset/wikivote/p_metis")
  //  PartitionEvaluator.cross("dataset/wikivote/edge", "dataset/wikivote/p_mip")(20)

}