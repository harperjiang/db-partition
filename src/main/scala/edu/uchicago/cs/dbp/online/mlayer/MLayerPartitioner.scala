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

    /*
     * Fetch or create a hyper vertex for the vertex 
     */
    var hvu = hvs.getOrElseUpdate(u.id, hyperfetch(u))
    var hvv = hvs.getOrElseUpdate(v.id, hyperfetch(v))

    var reassign = new ArrayBuffer[HyperVertex]();
    reassign += hvu += hvv

    /*
     * Merge two hyper vertices if necessary 
     */
    if (!hvu.equals(hvv) && shouldMerge(hvu, hvv)) {
      hvu.merge(hvv)
      reassign -= hvv
    }

    while (!reassign.isEmpty) {
      var hv = reassign.remove(0)
      if (assign(hv)) {
        reassign ++= hv.neighbors
      }
    }
  }

  /**
   * If a vertex doesn't have a hv, create one if it has enough neighbors, merge it to some neighbor otherwise
   * If a vertex already have a hv with more than one vertex, move it out if it has enough neighbors.
   */
  protected def hyperfetch(u: Vertex): HyperVertex = {

    var shouldIsolate = hasEnoughNeighbors(u);

    if (shouldIsolate) {
      var hv = hvs.getOrElse(u.id, null)
      if (null != hv && hv.size > 1) {
        hv.remove(u)
      }
      var newhv = new HyperVertex(u.id)
      newhv.add(u)
      hvs += ((u.id, newhv))
      return newhv;
    } else {
      // Find a neighbor hv to merge to
      var mergeto = u.neighbors
        .map(v => hvs.getOrElse(v.id, null))
        .filter(hv => hv != null).toSet.zipWithIndex
        .map(hvi => (joinScore(hvi._1), hvi._1))
        .maxBy(_._1)._2
      mergeto.add(u)
      return mergeto;
    }
  }

  /**
   * (Re)assign the hyper vertex to some partition if necessary.
   *
   * @return true if the hyper vertex is reassigned
   */
  protected def assign(hv: HyperVertex): Boolean = {
    false
  }

  protected def hasEnoughNeighbors(u: Vertex): Boolean = {
    false
  }

  /**
   * Vertex will choose the neighbor hvs having highest score to join
   */
  protected def joinScore(hv: HyperVertex): Double = {
    0
  }

  /**
   * Determine whether two hyper vertices should be merged together
   */
  protected def shouldMerge(hv1: HyperVertex, hv2: HyperVertex): Boolean = {
    false
  }

  class HyperVertex(vid: Int) {

    var id = vid;

    def size: Int = 0

    def neighbors: Iterable[HyperVertex] = {
      null
    }

    def add(u: Vertex) = {

    }

    def remove(u: Vertex) = {

    }

    def merge(hv: HyperVertex) = {

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

