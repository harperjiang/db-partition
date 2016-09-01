package edu.uchicago.cs.dbp.leopard.experiment.google

import scala.io.Source
import edu.uchicago.cs.dbp.leopard.Partitioner
import edu.uchicago.cs.dbp.leopard.model.Vertex
import edu.uchicago.cs.dbp.leopard.model.Edge
import edu.uchicago.cs.dbp.leopard.eval.PartitionPrinter
import java.io.FileOutputStream
import java.io.PrintWriter
import edu.uchicago.cs.dbp.leopard.Params
import scala.collection.mutable.HashSet
import scala.collection.mutable.HashMap
import edu.uchicago.cs.dbp.leopard.experiment.common.LeopardRunner
object RunLeopard extends App {

  Params.eSize = 1.7;
  Params.wSize = 0.005;
  Params.minReplica = 1;
  Params.avgReplica = 1;

  LeopardRunner.run("leopard/google/edge", 10, "leopard/google/p_leopard")
}