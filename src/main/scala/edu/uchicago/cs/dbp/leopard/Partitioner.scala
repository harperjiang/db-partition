package edu.uchicago.cs.dbp.leopard

import scala.Ordering
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Buffer
import scala.collection.mutable.HashSet
import scala.collection.mutable.ListBuffer

import edu.uchicago.cs.dbp.leopard.model.Edge
import edu.uchicago.cs.dbp.leopard.model.Partition
import edu.uchicago.cs.dbp.leopard.model.Vertex

class Partitioner(numPartition: Int) {

  private var partitions: Buffer[Partition] = new ArrayBuffer[Partition];

  private var scorer = new Scorer();

  private var slidingWindow = new ListBuffer[Double]();

  for (i <- 0 until numPartition) {
    partitions += new Partition(i);
  }

  def add(e: Edge): Unit = {
    e.vertices.foreach(_.attach(e.vertices));
    var reassignCandidates = new HashSet[Vertex]();
    // First assign all unassigned vertices
    e.vertices.foreach {
      v =>
        {
          if (v.primary == -1) { // Not assigned
            assign(v);
          } else {
            reassignCandidates += v;
          }
        }
    }
    // Trigger reassignment
    while (!reassignCandidates.isEmpty) {
      var v = reassignCandidates.iterator.next();
      var probReassign = (1 / Params.rescanProb - 1) / v.numNeighbors;

      if (probReassign > Params.rescanThreshold) {
        if (assign(v)) { // This vertex is reassigned, add all its immediate neighbors
          reassignCandidates ++= v.neighbors;
        }
      }
    }

  }

  /**
   * Return: true if this vertex is reassigned
   */
  def assign(v: Vertex): Boolean = {
    var oldAssign = v.primary;
    // Compute Score of v for each partition
    var pScores = v.numPrimaryNeighbors(numPartition);

    for (i <- 0 until numPartition) {
      var p = partitions(i);
      pScores(i) = pScores(i) - Params.wSize * Params.eSize * Math.pow(p.size, Params.eSize - 1) / 2;
    }

    var pMax = pScores.zipWithIndex.maxBy(_._1);

    // Primary Partition
    partitions(pMax._2).addPrimary(v);

    // Secondary Partition

    var sScores = v.numSecondaryNeighbors(numPartition);

    for (i <- 0 until numPartition) {
      var p = partitions(i);
      sScores(i) = sScores(i) - Params.wSize * Params.eSize * Math.pow(p.size, Params.eSize - 1) / 2;
    }

    var secSet = new HashSet[Int]();
    var sorted = sScores.zipWithIndex.sortBy(_._1)(Ordering[Double].reverse);

    // Add the first min - 1 copies to the secondary set
    sorted.dropRight(numPartition - Params.minReplica + 1).map(secSet += _._2);

    // Compute the average set
    while (slidingWindow.size >= Params.windowSize - numPartition) {
      slidingWindow.remove(0);
    }
    var buffer = new ArrayBuffer[(Double, Int, Boolean)]();
    slidingWindow.foreach(o => buffer += ((o, 0, false)));
    sScores.zipWithIndex.foreach(data => { buffer += ((data._1, data._2, true)) });

    var thres = ((Params.avgReplica - 1) / (numPartition - 1)).toInt;

    buffer.sortBy(_._1)(Ordering[Double].reverse).dropRight(buffer.size - thres).foreach(f => { if (f._3) secSet += f._2 });

    secSet.foreach { partitions(_).addSecondary(v) }

    return v.primary != oldAssign;
  }

}