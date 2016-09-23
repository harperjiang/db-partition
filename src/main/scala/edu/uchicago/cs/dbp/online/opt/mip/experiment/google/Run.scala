package edu.uchicago.cs.dbp.online.opt.mip.experiment.google

import edu.uchicago.cs.dbp.PartitionRunner
import edu.uchicago.cs.dbp.online.opt.mip.MIPPartitioner3
import edu.uchicago.cs.dbp.online.leopard.LeopardParams
import edu.uchicago.cs.dbp.online.opt.mip.MIPParams

object Run extends App {

  MIPParams.alpha = 10000f;
  MIPParams.beta = 1f;

  LeopardParams.eSize = 1.7;
  LeopardParams.wSize = 0.005;
  LeopardParams.minReplica = 1;
  LeopardParams.avgReplica = 1;

  PartitionRunner.run("dataset/google/edge", new MIPPartitioner3(10), "dataset/google/p_mip")
}