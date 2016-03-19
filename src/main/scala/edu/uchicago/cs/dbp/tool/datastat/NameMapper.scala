package edu.uchicago.cs.dbp.tool.datastat

object NameMapper {
  var mapping = Map("products" -> "P", "parts" -> "R", "suppliers" -> "S", "orders" -> "O", "district" -> "D", "stock" -> "K", "warehouse" -> "W", "customer" -> "C")
  
  def translate(dtype: String, dval: String): String = {
    return "%s%s".format(mapping.getOrElse(dtype, { throw new IllegalArgumentException(dtype); "" }), dval)
  }
}