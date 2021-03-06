package edu.uchicago.cs.dbp.online.leopard

import scala.Ordering
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Buffer
import scala.collection.mutable.HashSet
import scala.collection.mutable.ListBuffer
import scala.util.Random

import edu.uchicago.cs.dbp.Partitioner
import edu.uchicago.cs.dbp.model.Edge
import edu.uchicago.cs.dbp.model.Partition
import edu.uchicago.cs.dbp.model.Vertex
import edu.uchicago.cs.dbp.AbstractPartitioner

class LeopardPartitioner(numPartition: Int) extends AbstractPartitioner(numPartition) {

  private var slidingWindow = new ListBuffer[Double]();

  private var random = new Random(System.currentTimeMillis());

  def add(e: Edge): Unit = {
    var reassignCandidates = new HashSet[Vertex]();
    // First assign all unassigned vertices
    e.vertices.foreach {
      v =>
        {
          var reassign = assign(v)
          if (reassign) {
            reassignCandidates += v; // Already assigned, check for reassignment
          }
        }
    }
    // Trigger reassignment
    while (!reassignCandidates.isEmpty) {
      var v = reassignCandidates.iterator.next();
      reassignCandidates.remove(v);
      if (v.numNeighbors != 0) {
        var probReassign = (1 / LeopardParams.rescanProb - 1) / v.numNeighbors;

        var rand = random.nextDouble();

        if (rand <= probReassign) {
          if (assign(v)) { // This vertex is reassigned, add all its immediate neighbors
            reassignCandidates ++= v.neighbors;
          }
        }
      }
    }
  }

  /**
   * Return: true if this vertex is reassigned
   */
  def assign(v: Vertex): Boolean = {
    var oldAssign = v.primary;
    if (oldAssign != -1) {
      partitions(v.primary).removePrimary(v);
      v.replicas.foreach(partitions(_).removeSecondary(v));
    }
    // Compute Score of v for each partition
    var pScores = v.numPrimaryNeighbors(numPartition);

    for (i <- 0 until numPartition) {
      var p = partitions(i);
      pScores(i) = pScores(i) - LeopardParams.wSize * LeopardParams.eSize * Math.pow(p.size, LeopardParams.eSize - 1) / 2;
    }

    var pMax = pScores.zipWithIndex.maxBy(_._1);

    // Secondary Partition
    if (LeopardParams.avgReplica > LeopardParams.minReplica) {
      var sScores = v.numSecondaryNeighbors(numPartition);

      for (i <- 0 until numPartition) {
        var p = partitions(i);
        sScores(i) = sScores(i) - LeopardParams.wSize * LeopardParams.eSize * Math.pow(p.size, LeopardParams.eSize - 1) / 2;
      }

      var secSet = new HashSet[Int]();

      var remain = sScores.zipWithIndex.filter(_._2 != pMax._2);
      var sorted = remain.sortBy(_._1)(Ordering[Double].reverse);

      // Add the first min - 1 copies to the secondary set
      sorted.dropRight((numPartition - LeopardParams.minReplica).toInt).foreach(secSet += _._2);

      // Compute the average set
      while (slidingWindow.size >= LeopardParams.windowSize - numPartition) {
        slidingWindow.remove(0);
      }
      var buffer = new ArrayBuffer[(Double, Int, Boolean)]();
      slidingWindow.foreach(o => buffer += ((o, 0, false)));
      remain.foreach(data => { buffer += ((data._1, data._2, true)) });

      var thres = (((LeopardParams.avgReplica - 1) * buffer.size / (numPartition - 1))).toInt;

      buffer.sortBy(_._1)(Ordering[Double].reverse).dropRight(buffer.size - thres).foreach(f => { if (f._3) secSet += f._2 });

      slidingWindow ++= remain.map(_._1);

      secSet.foreach { partitions(_).addSecondary(v) }
    }

    partitions(pMax._2).addPrimary(v);

    return oldAssign != -1 && v.primary != oldAssign;
  }

}