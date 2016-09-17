package edu.uchicago.cs.dbp.online.opt.mip.experiment.wikivote

import edu.uchicago.cs.dbp.PartitionMerger

object Merge extends App {
  var mgr = new PartitionMerger();

  mgr.merge("dataset/wikivote/edge", "dataset/wikivote/p_mip", 20, 10)
}