package edu.uchicago.cs.dbp.online.mlayer

import edu.uchicago.cs.dbp.model.Edge
import edu.uchicago.cs.dbp.AbstractPartitioner
import edu.uchicago.cs.dbp.model.Vertex
import edu.uchicago.cs.dbp.model.Partition

/**
 * Add a HyperVertex layer between vertex and partition
 */
class MLayerPartitioner(nump: Int) extends AbstractPartitioner(nump) {

  /**
   * Mapping from vertex to its HyperVertex
   */
  var hvs = scala.collection.mutable.HashMap[Int,HyperVertex](); 

  def add(e: Edge) = {

  }
}

class HyperVertex(vid: Int) {

  var id = vid;

  var pid = 0;

  var vertices = new scala.collection.mutable.HashSet[Vertex];

  def merge(hv: HyperVertex) = {

  }
}