package edu.uchicago.cs.dbp.leopard.experiment.slashdot

import edu.uchicago.cs.dbp.leopard.Params
import edu.uchicago.cs.dbp.leopard.experiment.common.LeopardRunner
object RunLeopard extends App {

  Params.eSize = 1.7;
  Params.wSize = 0.15;
  Params.minReplica = 1;
  Params.avgReplica = 1;

  LeopardRunner.run("leopard/slashdot/edge", 10, "leopard/slashdot/p_leopard")
}