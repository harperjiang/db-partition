package edu.uchicago.cs.dbp.model

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Buffer
import scala.collection.mutable.HashSet
class Partition(pid: Int) {

  var id = pid;

  var vertices: scala.collection.mutable.Set[Vertex] = new HashSet[Vertex]();

  def size(): Int = vertices.size;

  def addPrimary(v: Vertex) = {
    vertices += v;
    v.assign(this.id);
  }

  def removePrimary(v: Vertex) = {
    v.assign(-1);
    vertices -= v;
  }

  def addSecondary(v: Vertex) = {
    vertices += v;
    v.addSecondary(this.id);
  }

  def removeSecondary(v: Vertex) = {
    vertices -= v;
    v.removeSecondary(this.id);
  }

  override def toString(): String = {
    return "%d:%d".format(id, size)
  }
}