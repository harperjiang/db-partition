package edu.uchicago.cs.dbp.online.opt.qp.experiment.wikivote

import edu.uchicago.cs.dbp.PartitionRunner
import edu.uchicago.cs.dbp.online.opt.qp.QPPartitioner
import edu.uchicago.cs.dbp.online.opt.qp.Params

object Run extends App {
  Params.alpha = 2000;
  Params.beta = 2f;

  PartitionRunner.run("dataset/wikivote/edge", new QPPartitioner(10), "dataset/wikivote/p_qp")
}