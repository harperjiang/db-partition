package edu.uchicago.cs.dbp.online.opt.mip.experiment.orkut

import edu.uchicago.cs.dbp.PartitionEvaluator

object Evaluate extends App {

  PartitionEvaluator.eval("dataset/orkut/edge", "dataset/orkut/p_leopard")
  PartitionEvaluator.eval("dataset/orkut/edge", "dataset/orkut/p_mip")
  PartitionEvaluator.eval("dataset/orkut/edge", "dataset/orkut/p_metis")

}