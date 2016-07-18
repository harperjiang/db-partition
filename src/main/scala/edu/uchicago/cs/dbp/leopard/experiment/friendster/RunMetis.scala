package edu.uchicago.cs.dbp.leopard.experiment.friendster

import java.io.FileOutputStream
import java.io.PrintWriter

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Buffer
import scala.collection.mutable.HashMap
import scala.io.Source
import scala.util.control._
import scala.collection.mutable.HashSet
import org.apache.hadoop.util.hash.Hash
import edu.uchicago.cs.dbp.leopard.experiment.common.MetisRunner

object RunMetis extends App {

  combineLine();

  def edgeDup() = {
    var out = new PrintWriter(new FileOutputStream("leopard/friendster/edge_dup"));
    Source.fromFile("leopard/friendster/edge").getLines().filter(!_.startsWith("#")).foreach(s => {
      var parts = s.split("\\s+");
      out.println("%d\t%d".format(parts(0).toInt, parts(1).toInt));
      out.println("%d\t%d".format(parts(1).toInt, parts(0).toInt));
    });

    out.close();
  }

  def genDict() = {
    var hash = new HashSet[Int]();
    var out = new PrintWriter(new FileOutputStream("leopard/friendster/metis_dict"));
    Source.fromFile("leopard/friendster/edge").getLines().filter(!_.startsWith("#")).foreach(s => {
      var parts = s.split("\\s+");
      hash += parts(0).toInt;
      hash += parts(1).toInt;
    });
    hash.foreach { out.println(_) }
    out.close();
  }

  def genDict2() = {
    var out = new PrintWriter(new FileOutputStream("leopard/friendster/metis_dict"));
    var count = 1;
    Source.fromFile("leopard/friendster/metis_dict_sort").getLines().filter(!_.startsWith("#")).foreach(s => {
      var key = s.toInt;
      out.println("%d\t%d".format(key, count));
      count += 1;
    });
    out.close();
  }

  def findSplit() = {
    var split = 6500000;
    var count = 1;
    Source.fromFile("leopard/friendster/metis_dict").getLines().filter(!_.startsWith("#")).foreach(s => {
      var keyv = s.split("\\s+");
      var key = keyv(0).toInt;
      var value = keyv(1).toInt;
      if (value % split == 0)
        System.out.println(key);
      count += 1;
    });
  }

  def splitFile() = {
    var numSplit = Array(11861110, 23618585, 35708488, 46010929, 56853685, 68458205, 81092819, 92957298, 105286294);
    var files = new ArrayBuffer[PrintWriter]();
    for (i <- 0 to 9)
      files += new PrintWriter(new FileOutputStream("leopard/friendster/edge_split_%d".format(i)));

    Source.fromFile("leopard/friendster/edge_dup").getLines().foreach(s => {
      var keyv = s.split("\\s+");
      var key = keyv(0).toInt;
      var index = 0;
      var i = 0;
      while (i < 9 && numSplit(i) < key) { i += 1 }
      files(i).println(s);
    });

    files.foreach(_.close());
  }

  def combineLine() = {
    var map = new HashMap[Int, Int]();
    Source.fromFile("leopard/friendster/metis_dict").getLines().foreach(s => {
      var parts = s.split("\\s+");
      var key = parts(0).toInt
      var value = parts(1).toInt
      map += ((key, value))
    });

    var output = new PrintWriter(new FileOutputStream("leopard/friendster/metis_input"))
    output.print("%d\t%d".format(65608366,1806067139));
    for (i <- 0 to 2) {
      var oldkey = -1;
      Source.fromFile("leopard/friendster/edge_split_%d.s".format(i)).getLines().foreach(s=>{
          var parts = s.split("\\s+");
          var from = parts(0).toInt
          var to = parts(1).toInt
          if(from != oldkey) {
            output.println();
            output.print(map.get(from).get);
            output.print("\t");
            output.print(map.get(to).get);
            oldkey = from;
          } else {
            output.print("\t");
            output.print(map.get(to).get);
          }
      });
    }

    output.close()
  }
}