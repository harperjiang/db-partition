package edu.uchicago.cs.dbp.leopard.experiment.notredame

import edu.uchicago.cs.dbp.leopard.Params
import edu.uchicago.cs.dbp.leopard.experiment.common.LeopardRunner
object RunLeopard extends App {

  Params.eSize = 1.7;
  Params.wSize = 0.008;
  Params.minReplica = 1;
  Params.avgReplica = 1;

  LeopardRunner.run("leopard/notredame/edge", 10, "leopard/notredame/p_leopard")
}