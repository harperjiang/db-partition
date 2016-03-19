package edu.uchicago.cs.dbp.partition.greedy1.mem

import scala.io.Source
object Main extends App {

  var graph = new Graph();

  Source.fromFile("input").getLines().foreach(line => {
    var parts = line.split("\t")
    graph.add(parts(0), parts(1))
  });
  
  
}