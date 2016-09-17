package edu.uchicago.cs.dbp.online.leopard.experiment.stanford

import edu.uchicago.cs.dbp.online.leopard.LeopardParams
import edu.uchicago.cs.dbp.online.leopard.experiment.common.LeopardRunner
object RunLeopard extends App {

  LeopardParams.eSize = 1.7;
  LeopardParams.wSize = 0.013;
  LeopardParams.minReplica = 1;
  LeopardParams.avgReplica = 1;

  LeopardRunner.run("leopard/stanford/edge", 10, "leopard/stanford/p_leopard")
}