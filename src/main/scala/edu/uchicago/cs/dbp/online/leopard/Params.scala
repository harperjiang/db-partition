package edu.uchicago.cs.dbp.online.leopard

import scala.collection.mutable.Stack
object LeopardParams {
  // The weight of partition size when calculating scores
  var wSize = 0.53d;

  var eSize = 1.5d;

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