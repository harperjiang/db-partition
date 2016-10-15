package edu.uchicago.cs.dbp.online.mlayer

import java.util.concurrent.atomic.AtomicInteger

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

  class HyperVertex(vid: Int) {

    var id = vid;

    var pid = -1;

    var neighbors = scala.collection.mutable.HashMap[HyperVertex, AtomicInteger]()

    var vertices = new scala.collection.mutable.HashSet[Vertex];

    /**
     * Assign a partition id to the hyper vertex
     */
    def assign(pid: Int) = {
      this.pid = pid;
    }

    /**
     * Remove the vertex from existing partitions (if any) and add it to the hyper-vertex's partition (if any)
     */
    def add(v: Vertex) = {
      vertices += v;

      // Handle Partitions
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
      // Handle neighbors
      var nhvo = hvs.get(v.id)
      if (!nhvo.isEmpty)
        neighbors.getOrElse(nhvo.get, new AtomicInteger(0)).incrementAndGet()
    }

    /**
     * Remove vertex from this hyper-vertex, but didn't change its partition (if any)
     */
    def remove(v: Vertex) = {
      vertices -= v
      // Remove neighbors
      var nhv = hvs.get(v.id).get

      if (0 == neighbors.get(nhv).get.decrementAndGet())
        neighbors.remove(nhv)
    }

    /**
     * Merge the target to current hyper vertex, discard the target
     */
    def merge(target: HyperVertex) = {

    }

    override def equals(obj: Any): Boolean = {
      if (obj.isInstanceOf[MLayerPartitioner.this.HyperVertex]) {
        return id == obj.asInstanceOf[MLayerPartitioner.this.HyperVertex].id
      }
      return super.equals(obj)
    }

    override def hashCode(): Int = id.hashCode()
  }
}

