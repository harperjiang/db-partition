package edu.uchicago.cs.dbp.model

import scala.collection.mutable.ArrayBuffer

class Edge(vs: Array[Vertex]) {

  def this(v1: Vertex, v2: Vertex) = {
    this(Array(v1, v2))
  }

  vs.foreach(_.attach(vs));

  def vertices = vs;
}