package edu.uchicago.cs.dbp.leopard.experiment.livejournal

import edu.uchicago.cs.dbp.leopard.Params
import edu.uchicago.cs.dbp.leopard.experiment.common.LeopardRunner
object RunLeopard extends App {

  Params.eSize = 1.7;
  Params.wSize = 0.004;
  Params.minReplica = 1;
  Params.avgReplica = 1;

  LeopardRunner.run("leopard/livejournal/edge", 10, "leopard/livejournal/p_leopard")
}