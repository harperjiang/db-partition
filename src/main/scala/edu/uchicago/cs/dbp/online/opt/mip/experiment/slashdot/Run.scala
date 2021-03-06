package edu.uchicago.cs.dbp.online.opt.mip.experiment.slashdot

import edu.uchicago.cs.dbp.PartitionRunner
import edu.uchicago.cs.dbp.online.opt.mip.MIPPartitioner3
import edu.uchicago.cs.dbp.online.leopard.LeopardParams
import edu.uchicago.cs.dbp.online.opt.mip.MIPParams

object Run extends App {

  MIPParams.alpha = 10000f;
  MIPParams.beta = 1f;

  LeopardParams.eSize = 1.7;
  LeopardParams.wSize = 0.15;
  LeopardParams.minReplica = 1;
  LeopardParams.avgReplica = 1;

  PartitionRunner.run("dataset/slashdot/edge", new MIPPartitioner3(10), "dataset/slashdot/p_mip")
}