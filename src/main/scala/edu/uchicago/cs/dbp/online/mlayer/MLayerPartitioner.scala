package edu.uchicago.cs.dbp.online.mlayer

import edu.uchicago.cs.dbp.model.{ Edge, Vertex }
import edu.uchicago.cs.dbp.AbstractPartitioner
import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import edu.uchicago.cs.dbp.online.ScoreFunc
import scala.collection.mutable.HashSet
import edu.uchicago.cs.dbp.model.Partition

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
    val u = e.vertices(0)
    val v = e.vertices(1)

    /*
     * Fetch or create a hyper vertex for the vertex 
     */
    var hvu = hvs.getOrElseUpdate(u.id, hyperFetch(u))
    var hvv = hvs.getOrElseUpdate(v.id, hyperFetch(v))

    /*
     * Merge two hyper vertices if necessary 
     */
    if (!hvu.equals(hvv) && shouldMerge(hvu, hvv)) {
      hvu.merge(hvv)
    }

    // Making assignment

    var assignHvu = assign(hvu, true)
    var assignHvv = assign(hvv, true)

    var reassign = new ArrayBuffer[HyperVertex]();
    if (assignHvu) reassign += hvu
    if (assignHvv) reassign += hvv

    while (!reassign.isEmpty) {
      val hv = reassign.remove(0)
      if (assign(hv, false)) {
        reassign ++= hv.neighbors
      }
    }
  }

  /**
   * If a vertex doesn't have a hv, create one if it has enough neighbors, merge it to some neighbor otherwise
   * If a vertex already have a hv with more than one vertex, move it out if it has enough neighbors.
   */
  protected def hyperFetch(u: Vertex): HyperVertex = {
    val shouldIsolate = hasEnoughNeighbors(u);

    if (shouldIsolate) {
      val hv = hvs.getOrElse(u.id, null)
      if (null != hv && hv.size > 1) {
        hv.remove(u)
        hv.updateNeighbors
      }
      val newhv = new HyperVertex(u.id)
      newhv.add(u)
      hvs += ((u.id, newhv))
      // Add this hv to all neighbors
      u.neighbors.map(v => hvs.getOrElse(v.id, null))
        .filter(_ != null).foreach(_.nhvs += hv)
      return newhv;
    } else {
      // Find a neighbor hv to merge to
      val mergeTo = u.neighbors
        .map(v => hvs.getOrElse(v.id, null))
        .filter(_ != null).zipWithIndex
        .map(hvi => (joinScore(u, hvi._1), hvi._1))
        .maxBy(_._1)._2
      mergeTo.add(u)
      return mergeTo;
    }
  }

  /**
   * (Re)assign the hyper vertex to some partition if necessary.
   *
   * @return true if the hyper vertex is reassigned
   */
  protected def assign(hv: HyperVertex, force: Boolean): Boolean = {
    if (hv.assigned == -1 || force || shouldReassign(hv)) {
      val oldAssign = hv.assigned

      val newAssign = hv.neighbors.toList
        .map(_.assigned).filter(_ != -1)
        .groupBy[Int](f => f)
        .map(f => (f._1, assignScore(f._2.size, partitions(f._1).size)))
        .maxBy(_._2)._1
      hv.assign(newAssign)
      oldAssign != newAssign
    } else {
      false
    }
  }

  /**
   * Current implementation just check a pre-defined threshold
   */
  protected def hasEnoughNeighbors(u: Vertex): Boolean = {
    u.neighbors.size >= MLayerParams.neighborThreshold
  }

  /**
   * Vertex will choose the neighbor hvs having highest score to join
   *
   * Current implementation is to find the hv with closest in-group average
   */
  protected def joinScore(v: Vertex, hv: HyperVertex): Double = {
    // First rule out hvs having single node with more than threshold neighbors
    isSingleOut(hv) match {
      case true => Double.MinValue
      case _ => -(hv.avgNumNeighbors - v.numNeighbors).abs
    }
  }

  protected def isSingleOut(hv: HyperVertex): Boolean = {
    hv.size == 1 && hv.avgNumNeighbors >= MLayerParams.neighborThreshold
  }

  /**
   * Determine whether two hyper vertices should be merged together
   */
  protected def shouldMerge(hv1: HyperVertex, hv2: HyperVertex): Boolean = {
    isSingleOut(hv1) || isSingleOut(hv2) match {
      case true => false
      case _ => {
        val ratio = (hv1.avgNumNeighbors / hv2.avgNumNeighbors)
        ratio < MLayerParams.mergeThreshold && ratio > (1 / MLayerParams.mergeThreshold)
      }
    }
  }

  /**
   * Determine whether should recalculate the reassignment of a hyper vertex
   * Current implementation choose to use the inverse of size
   */
  protected def shouldReassign(hv: HyperVertex): Boolean = {
    val probe = random.nextDouble()
    probe < (1 / MLayerParams.reassignRatio - 1) / hv.size;
  }

  /**
   * Partition with highest score will get this hv assigned
   */
  protected def assignScore(n: Int, psize: Int): Double = {
    ScoreFunc.leopardScore(n, psize)
  }

  class HyperVertex(vid: Int) {

    var id = vid;

    private var partition = -1

    private var vertices = HashSet[Vertex]()

    var nhvs = HashSet[HyperVertex]()

    def updateNeighbors = {
      nhvs = vertices.flatMap(_.neighbors)
        .map(v => hvs.getOrElse(v.id, null))
        .filter(hv => { hv != null && hv != this })
    }

    def neighbors: Iterable[HyperVertex] = nhvs

    def assigned: Int = partition

    // Should remove old assign and apply new assign if necessary
    def assign(p: Int) = {
      if (partition != -1) {
        vertices.foreach(u => {
          partitions(u.id).removePrimary(u)
        })
      }
      vertices.foreach(u => { partitions(p).addPrimary(u) })
      partition = p
    }

    def size: Int = vertices.size

    /**
     * The average number of neighbors for each vertex in the hv
     */
    def avgNumNeighbors: Double = {
      vertices.toList.map(_.numNeighbors).sum / vertices.size
    }

    def add(u: Vertex) = {
      vertices += u
      hvs += ((u.id, this))
      if (partition != u.primary) {
        if (u.primary != -1) {
          partitions(u.primary).removePrimary(u)
        }
        if (partition != -1) {
          partitions(partition).addPrimary(u)
        }
      }
    }

    def remove(u: Vertex) = {
      vertices -= u
      hvs -= u.id
      if (partition != -1) {
        partitions(partition).removePrimary(u)
      }
    }

    def merge(hv: HyperVertex) = {
      var hvp: Partition = null;
      var p: Partition = null
      if (hv.partition != -1)
        hvp = partitions(hv.partition)
      if (partition != -1)
        p = partitions(partition)
      vertices ++= hv.vertices
      hv.vertices.foreach(u => {
        if (hvp != null)
          hvp.removePrimary(u)
        if (p != null)
          p.addPrimary(u)
        hvs += ((u.id, this))
      })
      hv.neighbors.foreach { _.nhvs -= hv }
      nhvs ++= hv.neighbors
      nhvs -= this
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

