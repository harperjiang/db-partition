package edu.uchicago.cs.dbp.hadoop.common

import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper

import edu.uchicago.cs.dbp.hadoop.common.types.StringArrayWritable

class TextMapper(keyind: Array[Int]) extends Mapper[Object, Text, StringArrayWritable, StringArrayWritable] {

  var keyset = keyind.toSet;

  override def map(key: Object, value: Text, 
      context: Mapper[Object, Text, StringArrayWritable, StringArrayWritable]#Context) = {
    var input = value.toString().split("\t")

    var keys = new Array[String](keyind.length);
    var vals = new Array[String](input.length - keyind.length);

    var keycounter = 0
    var valcounter = 0

    for (i <- 0 to input.length - 1) {
      if (keyset(i)) {
        keys(keycounter) = input(i)
        keycounter += 1
      } else {
        vals(valcounter) = input(i)
        valcounter += 1
      }
    }
    context.write(new StringArrayWritable(keys), new StringArrayWritable(vals))
  }
}