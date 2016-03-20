package edu.uchicago.cs.dbp.partition.greedy1.mem

import scala.io.Source
import com.sun.glass.ui.Size
import java.io.FileOutputStream
import java.io.PrintWriter
import edu.uchicago.cs.dbp.tool.NameMapper
object Main extends App {

  var graph = new Graph();
  var partition = 10

  Source.fromFile("data/greedy1/transaction").getLines().foreach(line => {
    var parts = line.split("\t")
    graph.add(parts(0), NameMapper.translate(parts(1), parts(2)))
  });

  graph.partition(partition);

  var output = new PrintWriter(new FileOutputStream("data/greedy1/partition"))
  graph.labels.foreach(f => {
    var did = NameMapper.translate(f._1)
    output.println("%s\t%s\t%d".format(f._2, did._1, did._2))
  })
  output.close
}