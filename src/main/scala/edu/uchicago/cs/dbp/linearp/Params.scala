package edu.uchicago.cs.dbp.linearp

import scala.collection.mutable.Stack
object Params {

  var rescanProb = 0.5f
  
  var sigmoidLambda = 0.5f;

  private var stack = new Stack[(Double)]();

  def save(): Unit = {
    stack.push((0));
  }

  def load(): Unit = {
    var s = stack.pop();

  }
}