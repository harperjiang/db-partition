package edu.uchicago.cs.dbp.online.leopard.experiment.berkstan

import edu.uchicago.cs.dbp.online.leopard.LeopardParams
import edu.uchicago.cs.dbp.online.leopard.experiment.common.LeopardRunner
object RunLeopard extends App {

  LeopardParams.eSize = 1.7;
  LeopardParams.wSize = 0.012;
  LeopardParams.minReplica = 1;
  LeopardParams.avgReplica = 1;

  LeopardRunner.run("leopard/berkstan/edge", 10, "leopard/berkstan/p_leopard")
}