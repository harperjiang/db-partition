package edu.uchicago.cs.dbp.online.opt.mip

import scala.collection.mutable.Stack
object Params {

  var rescanProb = 0.5f

  var sigmoidLambda = 0.1f;

  var alpha = 500f;

  var beta = 1f;

  var rho = 1;

  var threshold = 2f;

  private var stack = new Stack[(Double)]();

  def save(): Unit = {
    stack.push((0));
  }

  def load(): Unit = {
    var s = stack.pop();

  }
}