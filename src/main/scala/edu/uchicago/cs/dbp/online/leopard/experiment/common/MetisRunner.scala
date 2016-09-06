package edu.uchicago.cs.dbp.online.leopard.experiment.common

import java.io.FileOutputStream
import java.io.PrintWriter
import scala.io.Source
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
object MetisRunner {
  def prepareInput(edgeFile: String, dictFile: String, minput: String): Unit = {
    var map = new HashMap[Int, scala.collection.mutable.Set[Int]]();

    Source.fromFile(edgeFile).getLines().filter(!_.startsWith("#")).foreach(s => {
      var parts = s.split("\\s+")
      var v1 = parts(0).toInt;
      var v2 = parts(1).toInt;

      if (v1 != v2) {
        map.getOrElseUpdate(v1, new HashSet[Int]()) += v2;
        map.getOrElseUpdate(v2, new HashSet[Int]()) += v1;
      }
    });

    map.foreach(f => { f._2.foreach { x => if (!map.get(x).get.contains(f._1)) throw new IllegalArgumentException("%d\t%d".format(f._1, x)) } })
    var edgeCounter = map.map(_._2.size).sum / 2;

    var vCount = map.size;

    var out = new PrintWriter(new FileOutputStream(minput));
    var dict = new PrintWriter(new FileOutputStream(dictFile));
    out.println("%d\t%d".format(vCount, edgeCounter));

    var counter = 1;

    var mapping = new HashMap[Int, Int]();

    var sorted = map.toList.sortBy(_._1);
    sorted.foreach(f => {
      var line = f._1;
      var data = f._2;
      if (line != counter) {
        mapping += (line -> counter);
        dict.println("%d\t%d".format(line, counter));
      }
      counter += 1;
    });
    sorted.foreach(f => {
      var line = f._1;
      var data = f._2;
      out.println(data.map(m => mapping.getOrElse(m, m)).mkString("\t"));
    });

    out.close();
    dict.close();
  }

  def prepareInputLarge(edgeFile: String, dictFile: String, minput: String, size: Int) = {

  }

  def translateInput(pfile: String, dictfile: String, outfile: String): Unit = {
    var dict = new HashMap[Int, Int]();

    var out = new PrintWriter(new FileOutputStream(pfile));
    Source.fromFile(dictfile).getLines().foreach(s => {
      var parts = s.split("\t");
      dict += ((parts(1).toInt -> parts(0).toInt));
    });
    var counter = 1;
    Source.fromFile(outfile).getLines().foreach(s => {
      var p = s.toInt;
      out.println("%d\t%d".format(p, dict.getOrElse(counter, counter)));
      counter += 1;
    });

    out.close();
  }
}