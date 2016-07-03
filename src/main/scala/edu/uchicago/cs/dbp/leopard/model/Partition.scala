package edu.uchicago.cs.dbp.leopard.model

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Buffer
class Partition(pid: Int) {

  var id = pid;

  var vertices: Buffer[Vertex] = new ArrayBuffer[Vertex]();

  def size(): Int = vertices.size;

  def addPrimary(v: Vertex) = {
    vertices += v;
    v.assign(this.id);
  }

  def addSecondary(v:Vertex) = {
    vertices += v;
    v.addSecondary(this.id);
  }
}