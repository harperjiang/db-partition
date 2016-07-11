package edu.uchicago.cs.dbp.leopard.experiment.wikivote

import scala.collection.mutable.HashMap
import scala.io.Source
object Evaluate extends App {

  eval("leopard/wikivote/edge", "leopard/wikivote/p_leopard")
  eval("leopard/wikivote/edge", "leopard/wikivote/p_metis")

  def eval(edge: String, partition: String) = {
    var counter = 0;
    var pmap = new HashMap[Int, Int]();

    var ps = new Array[Int](10);

    Source.fromFile(partition).getLines.foreach(s => {
      var parts = s.split("\\s+");
      pmap += ((parts(1).toInt, parts(0).toInt));
      ps(parts(0).toInt) += 1;
    });

    Source.fromFile(edge).getLines().filter(!_.startsWith("#")) foreach (s => {
      var parts = s.split("\\s+");
      var v1 = parts(0).toInt;
      var v2 = parts(1).toInt;
      if (pmap.get(v1).get != pmap.get(v2).get) {
        counter += 1;
      }
    });
    System.out.println(ps.mkString(","));
    System.out.println(counter);
  }
}