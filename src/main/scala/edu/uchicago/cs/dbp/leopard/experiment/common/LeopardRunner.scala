package edu.uchicago.cs.dbp.leopard.experiment.common

import java.io.FileOutputStream
import java.io.PrintWriter

import scala.collection.mutable.HashMap
import scala.io.Source

import edu.uchicago.cs.dbp.leopard.Partitioner
import edu.uchicago.cs.dbp.leopard.eval.PartitionPrinter
import edu.uchicago.cs.dbp.leopard.model.Edge
import edu.uchicago.cs.dbp.leopard.model.Vertex
object LeopardRunner {
  def run(edgeFile: String, nump: Int, outfile: String) = {
    var p = new Partitioner(nump);
    var vertices = new HashMap[Int, Vertex]();

    Source.fromFile(edgeFile).getLines().filter(!_.startsWith("#")).foreach(s => {
      var parts = s.split("\\s+")
      var v1id = parts(0).toInt;
      var v2id = parts(1).toInt;
      var v1 = vertices.getOrElseUpdate(v1id, new Vertex(v1id));
      var v2 = vertices.getOrElseUpdate(v2id, new Vertex(v2id));

      p.add(new Edge(Array(v1, v2)));
    });

    var out = new PrintWriter(new FileOutputStream(outfile));
    PartitionPrinter.print(p.partitions, out)
    out.close();
  }
}