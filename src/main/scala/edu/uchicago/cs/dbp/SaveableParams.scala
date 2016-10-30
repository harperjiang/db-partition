package edu.uchicago.cs.dbp

import scala.collection.mutable.Stack

abstract class SaveableParams[T] {

  private val stack = new Stack[T]()

  def save = stack.push(pack)
  def load = unpack(stack.pop)

  def pack: T
  def unpack(t: T): Unit
}