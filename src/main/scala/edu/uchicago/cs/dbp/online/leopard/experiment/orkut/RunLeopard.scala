package edu.uchicago.cs.dbp.online.leopard.experiment.orkut

import edu.uchicago.cs.dbp.online.leopard.LeopardParams
import edu.uchicago.cs.dbp.online.leopard.experiment.common.LeopardRunner
object RunLeopard extends App {

  LeopardParams.eSize = 1.7;
  LeopardParams.wSize = 0.022;
  LeopardParams.minReplica = 1;
  LeopardParams.avgReplica = 1;

  LeopardRunner.run("dataset/orkut/edge", 10, "dataset/orkut/p_leopard")
}