package edu.uchicago.cs.dbp.stg1.tool.datastat

trait LogProcessor {

  def addLine(tId: String, dtype: String, dId: String);
  def endTran(tId: String);
  def endProcess();
}