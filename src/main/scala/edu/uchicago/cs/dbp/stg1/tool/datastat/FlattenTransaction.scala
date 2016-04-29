package edu.uchicago.cs.dbp.stg1.tool.datastat

import java.io.PrintWriter
import edu.uchicago.cs.dbp.stg1.tool.NameMapper

class FlattenTransaction(output: PrintWriter) extends LogProcessor {

  var buffer = new scala.collection.mutable.HashMap[String, java.util.List[String]]()

  override def addLine(tId: String, dtype: String, dId: String) = {
    output.println("%s\t%s".format(tId, NameMapper.translate(dtype, dId)));
  }

  override def endTran(tId: String) = {
    output.flush();
  }

  override def endProcess() = {
    output.close();
  }
}