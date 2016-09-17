package edu.uchicago.cs.dbp.online.opt.mip.experiment.slashdot

import edu.uchicago.cs.dbp.PartitionEvaluator

object Evaluate extends App {

  PartitionEvaluator.eval("dataset/slashdot/edge", "dataset/slashdot/p_leopard")
  PartitionEvaluator.eval("dataset/slashdot/edge", "dataset/slashdot/p_mip")
  PartitionEvaluator.eval("dataset/slashdot/edge", "dataset/slashdot/p_metis")

}