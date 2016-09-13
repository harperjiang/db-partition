package edu.uchicago.cs.dbp

import scala.collection.mutable.HashMap
import scala.io.Source
object PartitionEvaluator {
  /**
   * Display the size of each partition and total number of cross-partition edges
   */
  def eval(edge: String, partition: String)(implicit pnum: Int = 10) = {
    var counter = 0;
    var pmap = new HashMap[Int, Int]();

    var ps = new Array[Int](pnum);

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

  /**
   * Show the number of cross-partition edges in form of triangular matrix
   */
  def cross(edge: String, partition: String)(implicit pnum: Int = 10) = {
    var result = Array.fill(pnum * (pnum - 1) / 2)(0)

    var pmap = new HashMap[Int, Int]();

    Source.fromFile(partition).getLines.foreach(s => {
      var parts = s.split("\\s+");
      pmap += ((parts(1).toInt, parts(0).toInt))
    })

    var tm = new TriangularMatrix(pnum - 1)

    Source.fromFile(edge).getLines().filter(!_.startsWith("#")) foreach (s => {
      var parts = s.split("\\s+");
      var v1 = parts(0).toInt;
      var v2 = parts(1).toInt;
      var p1 = pmap.get(v1).get
      var p2 = pmap.get(v2).get
      if (p1 != p2) {
        var large = Math.max(p1, p2)
        var small = p1 + p2 - large
        tm.add(small, large - 1, 1)
      }
    })
    tm.print
  }
}