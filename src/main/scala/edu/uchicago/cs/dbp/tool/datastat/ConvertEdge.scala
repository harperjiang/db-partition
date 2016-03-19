package edu.uchicago.cs.dbp.tool.datastat

import scala.collection.mutable.HashMap
import java.util.ArrayList
import scala.collection.JavaConversions._

class ConvertEdge extends LogProcessor {

  var mapping = Map("products" -> "P", "parts" -> "R", "suppliers" -> "S", "orders" -> "O", "district" -> "D", "stock" -> "K", "warehouse" -> "W", "customer" -> "C")

  var buffer = new scala.collection.mutable.HashMap[String, java.util.List[String]]()

  override def addLine(tId: String, dtype: String, dId: String) = {
    buffer.getOrElseUpdate(tId, new ArrayList[String]()).add(translate(dtype, dId))
  }

  override def endTran(tId: String) = {
    var listc = buffer.remove(tId)
    if (!listc.isEmpty) {
      var list = listc.get
      System.out.println("%s:%s".format(tId, list.mkString("\t")))
    }
  }

  override def endProcess() = {

  }

  def translate(dtype: String, dval: String): String = {
    return "%s%s".format(mapping.getOrElse(dtype, { throw new IllegalArgumentException(dtype); "" }), dval)
  }
}