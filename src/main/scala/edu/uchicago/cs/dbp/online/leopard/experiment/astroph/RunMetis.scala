package edu.uchicago.cs.dbp.online.leopard.experiment.astroph

import java.io.FileOutputStream
import java.io.PrintWriter

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Buffer
import scala.collection.mutable.HashMap
import scala.io.Source
import scala.collection.mutable.HashSet

object RunMetis extends App {
  translateInput();

  def prepareInput() = {
    var map = new HashMap[Int, scala.collection.mutable.Set[Int]]();

    Source.fromFile("leopard/astroph/edge").getLines().filter(!_.startsWith("#")).foreach(s => {
      var parts = s.split("\\s+")
      var v1 = parts(0).toInt;
      var v2 = parts(1).toInt;

      map.getOrElseUpdate(v1, new HashSet[Int]()) += v2;
      map.getOrElseUpdate(v2, new HashSet[Int]()) += v1;

    });

    var edgeCounter = map.map(_._2.size).sum / 2;

    var vCount = map.size;

    var out = new PrintWriter(new FileOutputStream("leopard/astroph/metis_input"));
    var dict = new PrintWriter(new FileOutputStream("leopard/astroph/metis_dict"));
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

  def translateInput(): Unit = {
    var dict = new HashMap[Int, Int]();

    var out = new PrintWriter(new FileOutputStream("leopard/astroph/p_metis"));
    Source.fromFile("leopard/astroph/metis_dict").getLines().foreach(s => {
      var parts = s.split("\t");
      dict += ((parts(1).toInt -> parts(0).toInt));
    });
    var counter = 1;
    Source.fromFile("leopard/astroph/metis_output").getLines().foreach(s => {
      var p = s.toInt;
      out.println("%d\t%d".format(p, dict.get(counter).get));
      counter += 1;
    });

    out.close();
  }
}