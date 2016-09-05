package edu.uchicago.cs.dbp.leopard.experiment.friendster

import edu.uchicago.cs.dbp.leopard.Params
import edu.uchicago.cs.dbp.leopard.experiment.common.LeopardRunner
object RunLeopard extends App {

  Params.eSize = 1.7;
  Params.wSize = 0.022;
  Params.minReplica = 1;
  Params.avgReplica = 1;

  LeopardRunner.run("leopard/friendster/edge", 10, "leopard/friendster/p_leopard")
}