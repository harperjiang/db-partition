package edu.clarkson.cs.itop.tool.common

import scala.collection.JavaConversions.iterableAsScalaIterable

import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.Reducer

object CounterParam {
  val KEY_INDEX = "counter.key_index";
}

class CounterMapper extends Mapper[Object, Text, Text, IntWritable] {
  val one = new IntWritable(1);
  val statickey = new Text("1");
  override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, IntWritable]#Context): Unit = {
    var keyIndex = context.getConfiguration.get(CounterParam.KEY_INDEX).toInt;
    if (keyIndex == -1) {
      // No Key Index, global counting
      context.write(statickey, one);
    } else {
      context.write(new Text(value.toString().split("\\s+")(keyIndex)), one)
    }
  }
}

class CounterReducer extends Reducer[Text, IntWritable, Text, IntWritable] {
  override def reduce(key: Text, values: java.lang.Iterable[IntWritable],
    context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
    var sum = 0;
    values.foreach(v => { sum += 1 });
    context.write(key, new IntWritable(sum))
  }
}