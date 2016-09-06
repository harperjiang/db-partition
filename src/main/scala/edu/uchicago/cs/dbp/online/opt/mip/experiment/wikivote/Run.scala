package edu.uchicago.cs.dbp.online.opt.mip.experiment.wikivote

import edu.uchicago.cs.dbp.PartitionRunner
import edu.uchicago.cs.dbp.online.opt.mip.MIPPartitioner

object Run extends App {
  PartitionRunner.run("dataset/wikivote/edge", new MIPPartitioner(10), "leopard/wikivote/p_mip")
}