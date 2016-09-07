package edu.uchicago.cs.dbp.online.opt.mip.experiment.astroph

import edu.uchicago.cs.dbp.PartitionRunner
import edu.uchicago.cs.dbp.online.opt.mip.MIPPartitioner

object Run extends App {
  PartitionRunner.run("dataset/astroph/edge", new MIPPartitioner(10), "dataset/astroph/p_mip")
}