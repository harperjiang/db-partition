package edu.uchicago.cs.dbp.online.leopard.experiment.wikivote

import scala.io.Source
import edu.uchicago.cs.dbp.online.leopard.LeopardPartitioner
import edu.uchicago.cs.dbp.model.Vertex
import edu.uchicago.cs.dbp.model.Edge
import edu.uchicago.cs.dbp.PartitionPrinter
import java.io.FileOutputStream
import java.io.PrintWriter
import edu.uchicago.cs.dbp.online.leopard.LeopardParams
import scala.collection.mutable.HashSet
import scala.collection.mutable.HashMap
object RunLeopard extends App {

  var p = new LeopardPartitioner(10);
  
  LeopardParams.eSize = 1.7;
  LeopardParams.wSize = 0.53d;
  LeopardParams.minReplica = 1;
  LeopardParams.avgReplica = 1;

  var vertices = new HashMap[Int,Vertex]();
  
  Source.fromFile("dataset/wikivote/edge").getLines().filter(!_.startsWith("#")).foreach(s => {
    var parts = s.split("\\s+")
    var v1id = parts(0).toInt;
    var v2id = parts(1).toInt;
    var v1 = vertices.getOrElseUpdate(v1id,new Vertex(v1id));
    var v2 = vertices.getOrElseUpdate(v2id,new Vertex(v2id));

    p.add(new Edge(Array(v1, v2)));
  });

  var out = new PrintWriter(new FileOutputStream("dataset/wikivote/p_leopard"));
  PartitionPrinter.print(p.partitions, out)
  out.close();
}