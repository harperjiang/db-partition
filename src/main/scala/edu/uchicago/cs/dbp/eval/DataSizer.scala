package edu.uchicago.cs.dbp.eval

trait DataSizer {

  def size(dataType: String): Int = {
    return 1;
  }

}

class DefaultDataSizer extends DataSizer {
  override def size(dataType:String): Int = {
    return 1;
  }
}