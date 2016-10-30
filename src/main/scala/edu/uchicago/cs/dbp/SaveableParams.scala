package edu.uchicago.cs.dbp

import java.util.Stack
import scala.reflect.runtime.universe._
import scala.reflect.runtime._
import scala.collection.mutable.ArrayBuffer
import scala.collection.Map
import scala.collection.mutable.HashMap

abstract class SaveableParams {

  private val stack = new Stack[Map[String, Any]]()

  def save = stack.push(pack)
  def load = unpack(stack.pop)

  private var loaded = false
  private val getters = new HashMap[String, MethodSymbol]
  private val setters = new HashMap[String, MethodSymbol]

  private def loadFields: Unit = {
    if (loaded)
      return
    currentMirror.classSymbol(this.getClass).toType.members.foreach {
      _ match {
        case m: MethodSymbol if m.isGetter && m.isPublic => {
          getters += ((m.accessed.name.toString, m))
        }
        case m: MethodSymbol if m.isSetter && m.isPublic => {
          setters += ((m.accessed.name.toString, m))
        }
        case _ =>
      }
    }
    loaded = true
  }

  private def pack: Map[String, Any] = {
    loadFields
    var im = currentMirror.reflect(this)
    return getters.mapValues { m => im.reflectMethod(m).apply() }.toMap
  }

  private def unpack(t: Map[String, Any]): Unit = {
    loadFields
    var im = currentMirror.reflect(this)
    t.foreach(entry => {
      if (setters.contains(entry._1))
        im.reflectMethod(setters.get(entry._1).get).apply(entry._2)
    })
  }
}