package edu.uchicago.cs.dbp.online.mlayer

import edu.uchicago.cs.dbp.AbstractPartitioner
import edu.uchicago.cs.dbp.model.Edge
import edu.uchicago.cs.dbp.model.Vertex

/**
 * Add a HyperVertex layer between vertex and partition
 */
class MLayerPartitioner(nump: Int) extends AbstractPartitioner(nump) {

  /**
   * Mapping from vertex to its HyperVertex
   */
  var hvs = scala.collection.mutable.HashMap[Int, HyperVertex]();

  def add(e: Edge) = {

  }

  object HyperVertex { def apply(vid:Int) = new HyperVertex(vid)}

  class HyperVertex(vid: Int) {

    var id = vid;

    var pid = -1;
    
    var neighbors = scala.collection.mutable.HashSet[HyperVertex]()

    var vertices = new scala.collection.mutable.HashSet[Vertex];

    /**
     * Remove the vertex from existing partitions (if any) and add it to the hyper-vertex's partition (if any)
     */
    def add(v: Vertex) = {
      vertices += v;

      if (pid != v.primary) {
        if (v.primary != -1) {
          var vp = partitions(v.primary)
          vp.removePrimary(v)
        }
        if (pid != -1) {
          var hvp = partitions(pid)
          hvp.addPrimary(v)
        }
      }
    }
    /**
     * Remove vertex from this hyper-vertex, but didn't change its partition (if any)
     */
    def remove(v: Vertex) = {
      vertices -= v
    }

    /**
     * Assign a partition id to the hyper vertex
     */
    def assign(pid:Int) = {
      
    }
    
    /**
     * Merge the target to current hyper vertex, discard the target
     */
    def merge(target: HyperVertex) = {
      
    }
  }
}

