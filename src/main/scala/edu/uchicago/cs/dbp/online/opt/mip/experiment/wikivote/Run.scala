package edu.uchicago.cs.dbp.online.opt.mip.experiment.wikivote

import edu.uchicago.cs.dbp.PartitionRunner
import edu.uchicago.cs.dbp.online.opt.mip.MIPPartitioner
import edu.uchicago.cs.dbp.online.opt.mip.Params
import edu.uchicago.cs.dbp.online.opt.mip.MIPPartitioner2

object Run extends App {
  Params.alpha = 10000f;
  Params.beta = 1f;

  PartitionRunner.run("dataset/wikivote/edge", new MIPPartitioner2(20), "dataset/wikivote/p_mip")
}