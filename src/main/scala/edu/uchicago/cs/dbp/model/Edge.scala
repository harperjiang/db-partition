package edu.uchicago.cs.dbp.model

import scala.collection.mutable.ArrayBuffer

class Edge(vs: Array[Vertex]) {

  vs.foreach(_.attach(vs));

  def vertices = vs;
}