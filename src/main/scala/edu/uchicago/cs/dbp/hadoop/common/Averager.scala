package edu.uchicago.cs.dbp.hadoop.common

import scala.collection.JavaConversions.iterableAsScalaIterable

import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.Reducer

object AveragerParam {
  val KEY_INDEX = "averager.key_index";
  val VAL_INDEX = "averager.value_index";
}

class AveragerMapper extends Mapper[Object, Text, Text, IntWritable] {

  val statickey = new Text("1");

  override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, IntWritable]#Context): Unit = {
    var keyIndex = context.getConfiguration.get(AveragerParam.KEY_INDEX).toInt;
    var valueIndex = context.getConfiguration.get(AveragerParam.VAL_INDEX).toInt;
    var parts = value.toString().split("\\s+");
    if (keyIndex == -1) {
      // No Key Index, global counting
      context.write(statickey, new IntWritable(parts(valueIndex).toInt));
    } else {
      context.write(new Text(parts(keyIndex)), new IntWritable(parts(valueIndex).toInt))
    }
  }
}

class AveragerReducer extends Reducer[Text, IntWritable, Text, Text] {
  override def reduce(key: Text, values: java.lang.Iterable[IntWritable],
    context: Reducer[Text, IntWritable, Text, Text]#Context): Unit = {
    var sum = 0d;
    var count = 0;
    values.foreach(v => { count += 1; sum += v.get() });
    context.write(key, new Text((sum / count).toString()));
  }
}