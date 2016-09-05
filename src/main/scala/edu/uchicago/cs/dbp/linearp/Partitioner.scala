package edu.uchicago.cs.dbp.linearp

import scala.Ordering
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Buffer
import scala.collection.mutable.HashSet
import scala.collection.mutable.ListBuffer

import edu.uchicago.cs.dbp.leopard.model.Edge
import edu.uchicago.cs.dbp.leopard.model.Partition
import edu.uchicago.cs.dbp.leopard.model.Vertex
import scala.util.Random
import ilog.cplex.IloCplex
import ilog.concert.IloIntExpr
import ilog.concert.IloNumVar
import scala.collection.mutable.HashMap

class Partitioner(numPartition: Int) {

  var partitions: Buffer[Partition] = new ArrayBuffer[Partition];

  private var slidingWindow = new ListBuffer[Double]();

  private var random = new Random(System.currentTimeMillis());

  for (i <- 0 until numPartition) {
    partitions += new Partition(i);
  }

  /**
   * Add an edge to the system and trigger partition assignment
   */
  def add(e: Edge): Unit = {
    e.vertices.foreach(_.attach(e.vertices));
    var reassignCandidates = new HashSet[Vertex]();
    // Compute the assignment for the edge
    var assignment = assign(e);
    e.vertices.foreach(v => {
      var assigned = assignment.get(v.id).get
      if (assigned) {
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
    var lset = genlset();

    var cplex = new IloCplex();

    // Create variables for u
    var uvars = cplex.intVarArray(p, 0, 1);
    // Create variables for v
    var vvars = cplex.intVarArray(p, 0, 1);

    // \sum x_{ui} = 1, \sum x_{vi} = 1
    var coef = Array.fill[Int](p)(1);
    cplex.addEq(1, cplex.scalProd(uvars, coef))
    cplex.addEq(1, cplex.scalProd(vvars, coef))

    // \forall i in L, x_{ui} = x_{vi} = 0
    lset.foreach(l => {
      cplex.addEq(0, uvars(l))
      cplex.addEq(0, vvars(l))
    })

    // Objective function
    var u = e.vertices(0);
    var uobjs = new Array[IloIntExpr](u.neighbors.size);
    u.neighbors.zipWithIndex.foreach(n => {
      uobjs(n._2) = uvars(n._1.primary);
    });
    var v = e.vertices(1);
    var vobjs = new Array[IloIntExpr](v.neighbors.size);
    v.neighbors.zipWithIndex.foreach(n => {
      vobjs(n._2) = vvars(n._1.primary);
    });

    cplex.addMaximize(cplex.sum(cplex.sum(uobjs), cplex.sum(vobjs)));

    if (!cplex.solve()) {
      throw new RuntimeException("LP not solvable");
    }

    var ures = cplex.getValues(uvars.map(_.asInstanceOf[IloNumVar]));
    var vres = cplex.getValues(vvars.map(_.asInstanceOf[IloNumVar]));

    var uassign = ures.zipWithIndex.filter(_._1 == 1)(0)._2;
    var vassign = vres.zipWithIndex.filter(_._1 == 1)(0)._2;

    var res = new HashMap[Int, Boolean]();

    res += (u.id -> (u.primary != -1 && u.primary != uassign));
    res += (v.id -> (v.primary != -1 && v.primary != vassign));
    return res.toMap;
  }

  /**
   * Construct LP and compute partition assignment for a vertex
   */
  def assign(u: Vertex): Boolean = {
    var p = partitions.size;
    var lset = genlset();

    var cplex = new IloCplex();

    // Create variables for u
    var uvars = cplex.intVarArray(p, 0, 1);

    // \sum x_{ui} = 1, \sum x_{vi} = 1
    var coef = Array.fill[Int](p)(1);
    cplex.addEq(1, cplex.scalProd(uvars, coef))

    // \forall i in L, x_{ui} = x_{vi} = 0
    lset.foreach(l => {
      cplex.addEq(0, uvars(l))
    })

    // Objective function
    var uobjs = new Array[IloIntExpr](u.neighbors.size);
    u.neighbors.zipWithIndex.foreach(n => {
      uobjs(n._2) = uvars(n._1.primary);
    });

    cplex.addMaximize(cplex.sum(uobjs));

    if (!cplex.solve()) {
      throw new RuntimeException("LP not solvable");
    }
    var ures = cplex.getValues(uvars.map(_.asInstanceOf[IloNumVar]));

    var uassign = ures.zipWithIndex.filter(_._1 == 1)(0)._2;

    return u.primary != -1 && u.primary != uassign;
  }

  /**
   * Generate the L set using sigmoid function
   */
  def genlset(): Set[Int] = {
    var avgSize = partitions.map(_.size).sum.toDouble / partitions.size;

    var min = partitions.zipWithIndex.min(Ordering.by[(Partition, Int), Int](_._1.size))

    var l = new HashSet[Int];

    partitions.foreach(p => {
      var prob = 1 / (1 + Math.exp(Params.sigmoidLambda * (avgSize - p.size)))
      var randval = random.nextDouble();
      if (prob >= randval) {
        l += p.id
      }
    })

    l.remove(min._1.id);
    return l.toSet;
  }
}