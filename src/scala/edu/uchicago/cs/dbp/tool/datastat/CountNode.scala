package edu.uchicago.cs.dbp.tool.datastat

import scala.io.Source
import java.util.regex.Pattern
import scala.collection.mutable.HashSet

class CountNode extends LogProcessor {

  var datastore = scala.collection.mutable.HashMap[String, HashSet[Int]]()

  override def addLine(tId: String, datatype: String, dval: String) = {
    var dataval = dval.toInt

    datastore.getOrElseUpdate(datatype, new HashSet[Int]).add(dataval)
  }

  override def endTran(tId: String) = {

  }

  override def endProcess() = {
    datastore.foreach(f => { System.out.println(f._1 + ":" + f._2.size) })
  }

}