package edu.uchicago.cs.dbp.partition.greedy1.mem

import scala.io.Source
import com.sun.glass.ui.Size
import java.io.FileOutputStream
import java.io.PrintWriter
object Main extends App {

  var graph = new Graph();
  var partition = 10

  Source.fromFile("data/greedy1/tran_obj").getLines().foreach(line => {
    var parts = line.split("\t")
    graph.add(parts(0), parts(1))
  });

  graph.partition(partition);

  var output = new PrintWriter(new FileOutputStream("data/greedy1/partition"))
  graph.labels.foreach(f => {
    output.println("%s\t%d".format(f._1, f._2))
  })
}