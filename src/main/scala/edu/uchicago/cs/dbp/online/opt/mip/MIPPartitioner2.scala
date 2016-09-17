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
import ilog.concert.IloNumExpr
import ilog.concert.IloNumVar
import ilog.cplex.IloCplex


/**
 * Use weight function to limit partition size
 */
class MIPPartitioner2(numPartition: Int) extends Partitioner {

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
        var probReassign = (1 / MIPParams.rescanProb - 1) / v.numNeighbors;

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

    var cplex = new IloCplex();
    cplex.setOut(null)

    // Create variables for u
    var uvars = cplex.intVarArray(p, 0, 1);
    // Create variables for v
    var vvars = cplex.intVarArray(p, 0, 1);

    // \sum x_{ui} = 1, \sum x_{vi} = 1
    var coef = Array.fill[Int](p)(1);
    cplex.addEq(1, cplex.scalProd(uvars, coef))
    cplex.addEq(1, cplex.scalProd(vvars, coef))

    // Objective function
    var uAssignedNeighbors = u.neighbors.filter(_.primary != -1);
    var uobjs = new Array[IloNumExpr](uAssignedNeighbors.size);
    uAssignedNeighbors.zipWithIndex.foreach(n => {
      var pn = n._1.primary;
      var p = partitions(pn)
      uobjs(n._2) = cplex.prod(weight(p.size), uvars(n._1.primary));
    });

    var vAssignedNeighbors = v.neighbors.filter(_.primary != -1);
    var vobjs = new Array[IloNumExpr](vAssignedNeighbors.size);
    vAssignedNeighbors.zipWithIndex.foreach(n => {
      var p = partitions(n._1.primary)
      vobjs(n._2) = cplex.prod(weight(p.size), vvars(n._1.primary));
    });

    var uvobjs = new Array[IloNumExpr](p);
    for (i <- 0 to p - 1) {
      uvobjs(i) = cplex.prod(weight(partitions(i).size), uvars(i), vvars(i))
    }

    cplex.addMaximize(cplex.sum(cplex.sum(uobjs), cplex.sum(vobjs), cplex.sum(uvobjs)));

    if (!cplex.solve()) {
      throw new RuntimeException("LP not solvable");
    }

    var ures = cplex.getValues(uvars.map(_.asInstanceOf[IloNumVar]));
    var vres = cplex.getValues(vvars.map(_.asInstanceOf[IloNumVar]));

    var uassign = ures.zipWithIndex.filter(_._1 == 1)(0)._2;
    var vassign = vres.zipWithIndex.filter(_._1 == 1)(0)._2;

    var res = new HashMap[Int, Boolean]();

    res += (u.id -> (olduAssign != -1 && olduAssign != uassign));
    res += (v.id -> (oldvAssign != -1 && oldvAssign != vassign));

    partitions(uassign).addPrimary(u)
    partitions(vassign).addPrimary(v)

    cplex.end()

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

    var cplex = new IloCplex();
    cplex.setOut(null)

    // Create variables for u
    var uvars = cplex.intVarArray(p, 0, 1);

    // \sum x_{ui} = 1, \sum x_{vi} = 1
    var coef = Array.fill[Int](p)(1);
    cplex.addEq(1, cplex.scalProd(uvars, coef))

    // Objective function
    var uobjs = new Array[IloNumExpr](u.neighbors.size);
    u.neighbors.zipWithIndex.foreach(n => {
      uobjs(n._2) = cplex.prod(weight(partitions(n._1.primary).size), uvars(n._1.primary));
    });

    cplex.addMaximize(cplex.sum(uobjs));

    if (!cplex.solve()) {
      throw new RuntimeException("LP not solvable");
    }
    var ures = cplex.getValues(uvars.map(_.asInstanceOf[IloNumVar]));

    var uassign = ures.zipWithIndex.filter(_._1 == 1)(0)._2;

    partitions(uassign).addPrimary(u)

    cplex.end()

    return oldAssign != -1 && oldAssign != uassign;
  }

  def weight(psize: Int): Double = {

    var avg = partitions.map(_.size).sum / numPartition;

    if (psize > avg * MIPParams.threshold)
      return 0;

    var x = psize + MIPParams.beta;
    return MIPParams.rho / (x * Math.log(MIPParams.alpha * x))
  }

}