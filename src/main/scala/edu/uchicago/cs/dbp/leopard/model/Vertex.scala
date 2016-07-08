package edu.uchicago.cs.dbp.leopard.model

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashSet
import scala.collection.mutable.HashMap
import scala.collection.mutable.Buffer
class Vertex extends Equals {
  var id: Int = 0;

  private var assignTo: Int = -1;

  private var secondary = new HashSet[Int]();

  var adj = new HashSet[Vertex]();

  def assign(p: Int): Unit = {
    this.assignTo = p;
  }

  def addSecondary(p: Int): Unit = {
    this.secondary += p;
  }

  def replicas = secondary;

  def primary = assignTo;

  def numPrimaryNeighbors(nump: Int): Array[Double] = {
    var result = Array.fill[Double](nump)(0);
    adj.foreach(v => {
      if (v.assignTo != -1) {
        result(v.assignTo) += 1;
        v.secondary.foreach { result(_) += 1; };
      }
    });
    return result;
  }

  def numSecondaryNeighbors(nump: Int): Array[Double] = {
    var result = Array.fill[Double](nump)(0);

    adj.foreach(v => {
      if (v.assignTo != -1) {
        result(v.assignTo) += 1;
      }
    });

    return result;
  }

  def neighbors: Iterable[Vertex] = adj;

  def numNeighbors = adj.size;

  def attach(vs: Iterable[Vertex]) = {
    adj ++= vs;
    adj.remove(this);
  }

  def canEqual(other: Any) = {
    other.isInstanceOf[edu.uchicago.cs.dbp.leopard.model.Vertex]
  }

  override def equals(other: Any) = {
    other match {
      case that: Vertex => this.id == other.asInstanceOf[Vertex].id
      case _ => false
    }
  }

  override def hashCode() = {
    val prime = 41
    prime * 13 + id.hashCode()
  }

}