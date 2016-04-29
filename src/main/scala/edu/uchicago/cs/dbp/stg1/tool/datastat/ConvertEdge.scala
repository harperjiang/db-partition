package edu.uchicago.cs.dbp.stg1.tool.datastat

import scala.collection.mutable.HashMap
import java.util.ArrayList
import scala.collection.JavaConversions._
import edu.uchicago.cs.dbp.stg1.tool.NameMapper

class ConvertEdge extends LogProcessor {

  var buffer = new scala.collection.mutable.HashMap[String, java.util.List[String]]()

  override def addLine(tId: String, dtype: String, dId: String) = {
    buffer.getOrElseUpdate(tId, new ArrayList[String]()).add(NameMapper.translate(dtype, dId))
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
}