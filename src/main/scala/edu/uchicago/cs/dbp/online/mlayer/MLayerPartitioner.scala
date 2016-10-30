package edu.uchicago.cs.dbp.online.mlayer

import edu.uchicago.cs.dbp.model.{ Edge, Vertex }
import edu.uchicago.cs.dbp.AbstractPartitioner
import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import edu.uchicago.cs.dbp.online.ScoreFunc
import scala.collection.mutable.HashSet
import edu.uchicago.cs.dbp.model.Partition
import java.util.concurrent.atomic.AtomicInteger

/**
 * Add a HyperVertex layer between vertex and partition
 */
class MLayerPartitioner(nump: Int) extends AbstractPartitioner(nump) {

  var random = new Random(System.currentTimeMillis())
  /**
   * Mapping from vertex to its HyperVertex
   */
  var hvs = scala.collection.mutable.HashMap[Int, HyperVertex]();

  /**
   *
   *
   *
   * TODO Need to reevaluate whether it is necessary to maintain a hyper vertex level neighbors
   */
  def add(e: Edge) = {
    val u = e.vertices(0)
    val v = e.vertices(1)

    /*
     * Fetch or create a hyper vertex for the vertex 
     */
    var hvu = hvs.getOrElseUpdate(u.id, hyperFetch(u))
    var hvv = hvs.getOrElseUpdate(v.id, hyperFetch(v))

    /*
     * Connect two hyper vertices, merge them if necessary 
     */
    if (!hvu.equals(hvv)) {
      shouldMerge(hvu, hvv) match {
        case true => hvu.merge(hvv)
        case false => hvu.connect(hvv)
      }
    }

    // Making assignment
    var reassignHvu = assign(hvu, true)
    var reassignHvv = hvv.equals(hvu) || hvv.size == 0 match {
      case true => false
      case false => assign(hvv, true)
    }

    var reassigned = new ArrayBuffer[HyperVertex]
    if (reassignHvu) reassigned ++= hvu.neighbors
    if (reassignHvv) reassigned ++= hvv.neighbors

    while (!reassigned.isEmpty) {
      val hv = reassigned.remove(0)
      if (assign(hv, false)) {
        reassigned ++= hv.neighbors
      }
    }
  }

  /**
   * If a vertex doesn't have a hv, create one if it has enough neighbors, merge it to some neighbor otherwise
   * If a vertex already have a hv with more than one vertex, move it out if it has enough neighbors.
   */
  private[mlayer] def hyperFetch(u: Vertex): HyperVertex = {
    // Either there is enough neighbors, or the vertex 
    // is new and has no known hv neighbor
    val shouldIsolate = hasEnoughNeighbors(u) ||
      (!hvs.contains(u.id)
        && u.neighbors.map(v => hvs.getOrElse(v.id, null))
        .filter(_ != null).isEmpty)

    if (shouldIsolate) {
      val hv = hvs.getOrElse(u.id, null)

      if (null != hv) {
        // Already has a hv
        hv.size match {
          case 1 => return hv
          case x if x > 1 => {
            hv.remove(u)
            // XXX update neighbors after removing a vertex
            hv.updateNeighbors
          }
        }
      }
      val newhv = new HyperVertex
      newhv.add(u)
      // Add this hv to all neighbors
      var nbs = u.neighbors.map(v => hvs.getOrElse(v.id, null))
        .filter(_ != null)
      // XXX update neighbors when creating a new hv
      newhv.nhvs ++= nbs
      nbs.foreach({ _.nhvs += newhv })
      return newhv;

    } else {
      // Already has a hv
      if (hvs.contains(u.id)) {
        return hvs.getOrElse(u.id, null)
      }
      // Find a neighbor hv to merge to
      val joinIn = u.neighbors
        .map(v => hvs.getOrElse(v.id, null))
        .filter(_ != null).zipWithIndex
        .map(hvi => (joinScore(u, hvi._1), hvi._1))
        .maxBy(_._1)._2
      joinIn.add(u)
      // XXX update neighbors after adding a new vertex to hv
      joinIn.nhvs ++= u.neighbors.map(v => hvs.getOrElse(v.id, null))
        .filter(_ != null) -= joinIn
      joinIn
    }
  }

  /**
   * (Re)assign the hyper vertex to some partition if necessary.
   *
   * @return true if the hyper vertex is reassigned
   */
  private[mlayer] def assign(hv: HyperVertex, force: Boolean): Boolean = {
    if (hv.assigned == -1 || force || shouldReassign(hv)) {
      val oldAssign = hv.assigned
      hv.assign(-1)
      val neighborSize = hv.vertices.flatMap(_.neighbors)
        .toList
        .map(_.primary).filter(_ != -1)
        .groupBy[Int](f => f).map(f => (f._1, f._2.size))
      val newAssign =
        {
          for (pinfo <- partitions.map(p => (p.id, p.size)))
            yield (pinfo._1,
            assignScore(neighborSize.getOrElse(pinfo._1, 0), pinfo._2))
        }.maxBy(_._2)._1
      hv.assign(newAssign)
      oldAssign != newAssign
    } else {
      false
    }
  }

  /**
   * Current implementation just check a pre-defined threshold
   */
  private[mlayer] def hasEnoughNeighbors(u: Vertex): Boolean = {
    u.neighbors.size >= MLayerParams.neighborThreshold
  }

  /**
   * Vertex will choose the neighbor hvs having highest score to join
   *
   * Current implementation is to find the hv with closest in-group average
   */
  private[mlayer] def joinScore(v: Vertex, hv: HyperVertex): Double = {
    // First rule out hvs having single node with more than threshold neighbors
    isSingleOut(hv) match {
      case true => Double.MinValue
      case _ => -(hv.avgNumNeighbors - v.numNeighbors).abs * hv.size
    }
  }

  private[mlayer] def isSingleOut(hv: HyperVertex): Boolean = {
    hv.size == 1 && hv.avgNumNeighbors >= MLayerParams.neighborThreshold
  }

  /**
   * Determine whether two hyper vertices should be merged together
   */
  private[mlayer] def shouldMerge(hv1: HyperVertex, hv2: HyperVertex): Boolean = {
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
  private[mlayer] def shouldReassign(hv: HyperVertex): Boolean = {
    val probe = random.nextDouble()
    probe < ((1 / MLayerParams.reassignRatio - 1) / hv.neighbors.size);
  }

  /**
   * Partition with highest score will get this hv assigned
   */
  private[mlayer] def assignScore(n: Int, psize: Int): Double = {
    ScoreFunc.leopardScore(n, psize)
  }

  private var hvCounter = new AtomicInteger()
  /**
   * The 'group' of vertices
   *
   * Most of the operations are well-encapsulated. The only
   * exception is the hv neighbors, for which
   * this class itself doesn't have enough information
   * to maintain. Thus we allow <code>MLayerPartitioner</code>
   * to directly modify the value
   */
  private[mlayer] class HyperVertex {

    var id = hvCounter.getAndIncrement;

    var partition = -1

    var vertices = HashSet[Vertex]()

    var nhvs = HashSet[HyperVertex]()

    var avgNumNeighbors: Double = 0

    def updateNeighbors = {
      nhvs = vertices.flatMap(_.neighbors)
        .map(v => hvs.getOrElse(v.id, null))
        .filter(hv => { hv != null && hv != this })
    }

    def neighbors: Iterable[HyperVertex] = nhvs

    def assigned: Int = partition

    // Should remove old assign and apply new assign if necessary
    def assign(p: Int): Unit = {
      if (p == partition)
        return
      if (partition != -1)
        vertices.foreach(u => partitions(u.primary).removePrimary(u))

      if (p != -1)
        vertices.foreach(u => { partitions(p).addPrimary(u) })
      partition = p
    }

    def size: Int = vertices.size

    def add(u: Vertex) = {
      // Recompute avg num neighbor
      val numNewEdge = u.neighbors.toList.map { v => hvs.getOrElse(v.id, null) }
        .filter { _ == this }.size + u.numNeighbors
      avgNumNeighbors = (avgNumNeighbors * size + numNewEdge) / (size + 1)
      // Data Structure
      vertices += u
      hvs += ((u.id, this))
      // Partitions
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
      // Recompute avg num neighbor
      size match {
        case 1 => avgNumNeighbors = 0
        case _ => {
          avgNumNeighbors = (avgNumNeighbors * size - u.numNeighbors) / (size - 1)
        }
      }
      // Data Structure
      vertices -= u
      hvs -= u.id
      // Partition
      if (partition != -1) {
        partitions(partition).removePrimary(u)
      }
    }

    def merge(hv: HyperVertex) = {
      // Recompute avg num neighbor
      avgNumNeighbors = (avgNumNeighbors * size + hv.avgNumNeighbors * hv.size) / (size + hv.size)

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
      hv.vertices.clear
      // XXX update neighbors when mergeing two hvs
      hv.neighbors.foreach { n => n.nhvs -= hv += this }
      nhvs ++= hv.neighbors
      nhvs -= this
    }

    def connect(hv: HyperVertex) = {
      // XXX update neighbors when connecting two hvs 
      this.nhvs += hv
      hv.nhvs += this
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

