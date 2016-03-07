package edu.uchicago.cs.dbp.common

import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.io.Writable
import org.apache.hadoop.io.Text
import scala.collection.immutable.HashSet

class TextMapper(keyind: Array[Int]) extends Mapper[Object, Text, Array[Text], Array[Text]] {

  var keyset = keyind.toSet;

  override def map(key: Object, value: Text, context: Mapper[Object, Text, Array[Text], Array[Text]]#Context) = {
    var input = value.toString().split("\t")

    var keys = new Array[Text](keyind.length);
    var vals = new Array[Text](input.length - keyind.length);

    var keycounter = 0
    var valcounter = 0

    for (i <- 0 to input.length - 1) {
      if (keyset(i)) {
        keys(keycounter) = new Text(input(i))
        keycounter += 1
      } else {
        vals(valcounter) = new Text(input(i))
        valcounter += 1
      }
    }
    context.write(keys, vals)
  }
}