package edu.uchicago.cs.dbp.online.opt.mip

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Buffer
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import scala.collection.mutable.ListBuffer
import scala.util.Random

import edu.uchicago.cs.dbp.Partitioner
import edu.uchicago.cs.dbp.model.Edge
import edu.uchicago.cs.dbp.model.Partition
import edu.uchicago.cs.dbp.model.Vertex
import ilog.concert.IloIntExpr
import ilog.concert.IloNumVar
import ilog.cplex.IloCplex
import ilog.concert.IloNumExpr

/**
 * Instead of invoking CPLEX to solve ILP, compute the score for each partition
 */

class MIPPartitioner3(numPartition: Int) extends Partitioner {

  var partitions: Buffer[Partition] = new ArrayBuffer[Partition];

  private var random = new Random(System.currentTimeMillis());

  for (i <- 0 until numPartition) {
    partitions += new Partition(i);
  }

  /**
   * Add an edge to the system and trigger partition assignment
   */
  def add(e: Edge): Unit = {
    var reassignCandidates = new HashSet[Vertex]();
    // Compute the assignment for the edge
    var assignment = assign(e);
    e.vertices.foreach(v => {
      var reassign = assignment.get(v.id).get
      if (reassign) {
        reassignCandidates += v;
      }
    });

    // Check whether to trigger reassignment
    while (!reassignCandidates.isEmpty) {
      var v = reassignCandidates.iterator.next();
      reassignCandidates.remove(v);
      if (v.numNeighbors != 0) {
        var probReassign = (1 / Params.rescanProb - 1) / v.numNeighbors;

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
   * Construct LP and compute partition assignment for an edge
   */
  def assign(e: Edge): Map[Int, Boolean] = {
    var p = partitions.size;

    var u = e.vertices(0);
    var v = e.vertices(1);
    // Remove old assignment

    var olduAssign = u.primary;
    if (olduAssign != -1) {
      partitions(u.primary).removePrimary(u);
    }

    var oldvAssign = v.primary;
    if (oldvAssign != -1) {
      partitions(v.primary).removePrimary(v);
    }

    // Compute assignment value for u and v
    var uNeighbors = u.numPrimaryNeighbors(numPartition)
    var vNeighbors = v.numPrimaryNeighbors(numPartition)
    var psize = partitions.map(_.size)

    var uScore = uNeighbors.zip(psize).map(t => score(t._1, t._2))
    var vScore = vNeighbors.zip(psize).map(t => score(t._1, t._2))

    var scores = Array.ofDim[Double](numPartition, numPartition)

    // Looking for the maximal
    var max = Double.MinValue
    var candid = (-1, -1)
    var single = Array.fill(numPartition)(1).zip(psize).map(t => score(t._1, t._2))
    for (i <- 0 until numPartition) {
      for (j <- 0 until numPartition) {
        scores(i)(j) = uScore(i) + vScore(j)
        if (i == j)
          scores(i)(j) += single(i)
        if (scores(i)(j) > max) {
          max = scores(i)(j)
          candid = (i, j)
        }
      }
    }

    var uassign = candid._1
    var vassign = candid._2

    var res = new HashMap[Int, Boolean]();

    res += (u.id -> (olduAssign != -1 && olduAssign != uassign));
    res += (v.id -> (oldvAssign != -1 && oldvAssign != vassign));

    partitions(uassign).addPrimary(u)
    partitions(vassign).addPrimary(v)

    return res.toMap;
  }

  /**
   * Construct LP and compute partition assignment for a vertex
   */
  def assign(u: Vertex): Boolean = {
    var p = partitions.size;

    // Remove old assignment
    var oldAssign = u.primary;
    if (oldAssign != -1) {
      partitions(u.primary).removePrimary(u);
    }

    var neighbors = u.numPrimaryNeighbors(numPartition)
    var psizes = partitions.map(_.size)

    var nweights = neighbors.zip(psizes).map(t => score(t._1, t._2))

    var maxWeight = nweights.zipWithIndex.max(Ordering.by[(Double, Int), Double](_._1))

    var uassign = maxWeight._2

    partitions(uassign).addPrimary(u)

    return oldAssign != -1 && oldAssign != uassign;
  }

  def weight(psize: Int): Double = {

    var avg = partitions.map(_.size).sum / numPartition;

    if (psize > avg * Params.threshold)
      return 0;

    var x = psize + Params.beta;
    return Params.rho / (x * Math.log(Params.alpha * x))
  }

  def score(n: Double, psize: Int): Double = {
    n * weight(psize)
  }
}