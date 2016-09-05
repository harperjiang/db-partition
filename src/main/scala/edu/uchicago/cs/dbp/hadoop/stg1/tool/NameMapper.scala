package edu.uchicago.cs.dbp.hadoop.stg1.tool

object NameMapper {
  var mapping = Map("products" -> "P", "parts" -> "R", "suppliers" -> "S", "orders" -> "O", "district" -> "D", "stock" -> "K", "warehouse" -> "W", "customer" -> "C")

  var rmapping = Map("P" -> "products", "R" -> "parts", "S" -> "suppliers", "O" -> "orders", "D" -> "district", "K" -> "stock", "W" -> "warehouse", "C" -> "customer")

  def translate(dtype: String, dval: String): String = {
    return "%s%s".format(mapping.getOrElse(dtype, { throw new IllegalArgumentException(dtype); "" }), dval)
  }

  def translate(did: String): (String, String) = {
    var dtype = rmapping.getOrElse(did.substring(0, 1), { throw new IllegalArgumentException(did); "" })
    var dval = did.substring(1)
    return (dtype, dval)
  }
}