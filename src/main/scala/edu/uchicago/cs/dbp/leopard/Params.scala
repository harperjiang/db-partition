package edu.uchicago.cs.dbp.leopard

import scala.collection.mutable.Stack
object Params {
  // The weight of partition size when calculating scores
  var wSize = 1d;

  var eSize = 3d;

  var rescanProb = 0.5d;

  var rescanThreshold = 0.5d;

  var minReplica = 2d;

  var avgReplica = 3.5d;

  var windowSize = 20d;

  private var stack = new Stack[(Double, Double, Double, Double, Double, Double, Double)]();

  def save(): Unit = {
    stack.push((wSize, eSize, rescanProb, rescanThreshold, minReplica, avgReplica, windowSize));
  }

  def load(): Unit = {
    var s = stack.pop();
    wSize = s._1;
    eSize = s._2;
    rescanProb = s._3;
    rescanThreshold = s._4;
    minReplica = s._5;
    avgReplica = s._6;
    windowSize = s._7;
  }
}