package edu.uchicago.cs.dbp.online.leopard.experiment.google

import edu.uchicago.cs.dbp.online.leopard.Params
import edu.uchicago.cs.dbp.online.leopard.experiment.common.LeopardRunner
object RunLeopard extends App {

  Params.eSize = 1.7;
  Params.wSize = 0.005;
  Params.minReplica = 1;
  Params.avgReplica = 1;

  LeopardRunner.run("leopard/google/edge", 10, "leopard/google/p_leopard")
}