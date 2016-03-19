package edu.clarkson.cs.itop.tool.common

import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.NullWritable

class ConcatMapper extends Mapper[Object, Text, Text, NullWritable] {

  override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, NullWritable]#Context) = {
    context.write(value, NullWritable.get);
  }
}