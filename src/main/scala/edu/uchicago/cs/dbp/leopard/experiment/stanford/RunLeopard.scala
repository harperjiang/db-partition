package edu.uchicago.cs.dbp.leopard.experiment.stanford

import edu.uchicago.cs.dbp.leopard.Params
import edu.uchicago.cs.dbp.leopard.experiment.common.LeopardRunner
object RunLeopard extends App {

  Params.eSize = 1.7;
  Params.wSize = 0.013;
  Params.minReplica = 1;
  Params.avgReplica = 1;

  LeopardRunner.run("leopard/stanford/edge", 10, "leopard/stanford/p_leopard")
}