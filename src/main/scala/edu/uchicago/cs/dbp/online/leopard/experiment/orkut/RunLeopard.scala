package edu.uchicago.cs.dbp.online.leopard.experiment.orkut

import edu.uchicago.cs.dbp.online.leopard.Params
import edu.uchicago.cs.dbp.online.leopard.experiment.common.LeopardRunner
object RunLeopard extends App {

  Params.eSize = 1.7;
  Params.wSize = 0.022;
  Params.minReplica = 1;
  Params.avgReplica = 1;

  LeopardRunner.run("leopard/orkut/edge", 10, "leopard/orkut/p_leopard")
}