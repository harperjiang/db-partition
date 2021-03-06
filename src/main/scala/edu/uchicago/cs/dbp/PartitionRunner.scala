package edu.uchicago.cs.dbp

import java.io.FileOutputStream
import java.io.PrintWriter
import scala.collection.mutable.HashMap
import scala.io.Source
import edu.uchicago.cs.dbp.model.Edge
import edu.uchicago.cs.dbp.model.Vertex
import scala.collection.mutable.HashSet

object PartitionRunner {
  def run(edgeFile: String, p: Partitioner, outfile: String) = {

    Source.fromFile(edgeFile).getLines().filter(!_.startsWith("#")).foreach(s => {
      var parts = s.split("\\s+")
      var v1id = parts(0).toInt;
      var v2id = parts(1).toInt;
      var v1 = new Vertex(v1id);
      var v2 = new Vertex(v2id);
      p.add(new Edge(Array(v1, v2)));
    });
    p.done
    var out = new PrintWriter(new FileOutputStream(outfile));
    PartitionPrinter.print(p.partitions, out)
    out.close();
  }
}