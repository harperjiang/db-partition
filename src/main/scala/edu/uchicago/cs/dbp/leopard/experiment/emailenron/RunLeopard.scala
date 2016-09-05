package edu.uchicago.cs.dbp.leopard.experiment.emailenron

import java.io.FileOutputStream
import java.io.PrintWriter

import scala.collection.mutable.HashMap
import scala.io.Source

import edu.uchicago.cs.dbp.leopard.Params
import edu.uchicago.cs.dbp.leopard.eval.PartitionPrinter
import edu.uchicago.cs.dbp.model.Vertex
import edu.uchicago.cs.dbp.leopard.LeopardPartitioner
import edu.uchicago.cs.dbp.model.Edge

object RunLeopard extends App {

  var p = new LeopardPartitioner(10);
  
  Params.eSize = 1.7;
  Params.wSize = 0.17;
  Params.minReplica = 1;
  Params.avgReplica = 1;

  var vertices = new HashMap[Int,Vertex]();
  
  Source.fromFile("leopard/emailenron/edge").getLines().filter(!_.startsWith("#")).foreach(s => {
    var parts = s.split("\\s+")
    var v1id = parts(0).toInt;
    var v2id = parts(1).toInt;
    var v1 = vertices.getOrElseUpdate(v1id,new Vertex(v1id));
    var v2 = vertices.getOrElseUpdate(v2id,new Vertex(v2id));

    p.add(new Edge(Array(v1, v2)));
  });

  var out = new PrintWriter(new FileOutputStream("leopard/emailenron/p_leopard"));
  PartitionPrinter.print(p.partitions, out)
  out.close();
}