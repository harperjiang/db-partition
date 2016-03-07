package edu.clarkson.cs.itop.tool.common

import scala.collection.JavaConversions.iterableAsScalaIterable

import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.Reducer

object SumParam {
  val KEY_INDEX = "sum.key_index";
  val VAL_INDEX = "sum.value_index";
}

class SumMapper extends Mapper[Object, Text, Text, IntWritable] {

  override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, IntWritable]#Context): Unit = {
    var keyIndex = context.getConfiguration.get(SumParam.KEY_INDEX).toInt;
    var valueIndex = context.getConfiguration.get(SumParam.VAL_INDEX).toInt;
    var parts = value.toString().split("\\s+");

    context.write(new Text(parts(keyIndex)), new IntWritable(parts(valueIndex).toInt))
  }
}

class SumReducer extends Reducer[Text, IntWritable, Text, Text] {
  override def reduce(key: Text, values: java.lang.Iterable[IntWritable],
    context: Reducer[Text, IntWritable, Text, Text]#Context): Unit = {
    var sum = 0d;
    values.foreach(v => { sum += v.get() });
    context.write(key, new Text(sum.toString()));
  }
}