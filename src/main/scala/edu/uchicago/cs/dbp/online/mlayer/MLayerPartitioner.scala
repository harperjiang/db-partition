package edu.uchicago.cs.dbp.online.mlayer

import java.util.concurrent.atomic.AtomicInteger

import edu.uchicago.cs.dbp.AbstractPartitioner
import edu.uchicago.cs.dbp.model.Edge
import edu.uchicago.cs.dbp.model.Vertex
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
 * Add a HyperVertex layer between vertex and partition
 */
class MLayerPartitioner(nump: Int) extends AbstractPartitioner(nump) {

  var random = new Random(System.currentTimeMillis())
  /**
   * Mapping from vertex to its HyperVertex
   */
  var hvs = scala.collection.mutable.HashMap[Int, HyperVertex]();

  def add(e: Edge) = {
    var u = e.vertices(0)
    var v = e.vertices(1)

    // Get hyper vertices
    def hypergen(v: Vertex): HyperVertex = {
      var hv = new HyperVertex(v.id)
      hv.add(v)
      hv
    }
    /*
     * If a vertex doesn't have a hv, create one if it has enough neighbors, merge it to some neighbor otherwise
     * If a vertex already have a hv with more than one vertex, move it out if it has enough neighbors.
     * If the new edge bring in new neighbors for existing hvs, combine the hvs if possible.
     */
    var hvu = resite(u)
    var hvv = resite(v)

    var reassign = new ArrayBuffer[HyperVertex]();
    reassign ++= Array(hvu, hvv, newhvu, newhvv)

    while (!reassign.isEmpty) {
      var hv = reassign.remove(0)
      if (assign(hv)) {
        reassign ++= hv.neighbors
      }
    }
  }

  /**
   * Move the vertex between hyper vertices if necessary.
   *
   *
   */
  protected def resite(v: Vertex): HyperVertex = {
    null
    // Use a score function to determine whether
  }

  /**
   * (Re)assign the hyper vertex to some partition if necessary.
   *
   * @return true if the hyper vertex is reassigned
   */
  protected def assign(hv: HyperVertex): Boolean = {
    false
  }

  class HyperVertex(vid: Int) {

    var id = vid;

    def neighbors: Iterator[HyperVertex] = {
      null
    }

    def add(v: Vertex) = {
      null
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

