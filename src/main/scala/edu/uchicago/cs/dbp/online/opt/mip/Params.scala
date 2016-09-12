package edu.uchicago.cs.dbp.online.opt.mip

import scala.collection.mutable.Stack
object Params {

  var rescanProb = 0.5f
  
  var sigmoidLambda = 0.1f;
  
  var alpha = 5000f;
  
  var beta = 1f;

  private var stack = new Stack[(Double)]();

  def save(): Unit = {
    stack.push((0));
  }

  def load(): Unit = {
    var s = stack.pop();

  }
}