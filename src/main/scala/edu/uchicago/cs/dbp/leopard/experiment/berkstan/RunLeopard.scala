package edu.uchicago.cs.dbp.leopard.experiment.berkstan

import edu.uchicago.cs.dbp.leopard.Params
import edu.uchicago.cs.dbp.leopard.experiment.common.LeopardRunner
object RunLeopard extends App {

  Params.eSize = 1.7;
  Params.wSize = 0.012;
  Params.minReplica = 1;
  Params.avgReplica = 1;

  LeopardRunner.run("leopard/berkstan/edge", 10, "leopard/berkstan/p_leopard")
}